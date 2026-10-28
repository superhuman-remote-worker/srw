"""Preserve table HTTP contracts across the main-to-router extraction.

Identity resolution and database I/O are doubled; approval, the admin gate,
audit enrichment, request parsing and response serialization remain real.
The assembled application receives this router's app-owned database dependency;
its real middleware still determines generic error responses.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import HTTPException
from fastapi.openapi.utils import get_openapi

from orchestrator.security import auth


_TABLE_PATHS = (
    "/api/tables",
    "/api/tables/users",
    "/api/tables/users/schema",
)
_DENIED_PATHS = (
    *_TABLE_PATHS,
    "/api/tables/not_an_allowed_table",
    "/api/tables/not_an_allowed_table/schema",
)


@pytest.fixture
def tables_api(monkeypatch, user_admin):
    from orchestrator import main
    from orchestrator.routers.tables import TablesDependencies

    columns = [{"name": "id", "type": "string", "nullable": False}]
    db = SimpleNamespace(
        get_tables=AsyncMock(return_value=[{"name": "users", "rowCount": 1}]),
        get_table_schema=AsyncMock(return_value=columns),
        get_table_data=AsyncMock(
            return_value={
                "columns": columns,
                "rows": [{"id": user_admin["id"]}],
                "total": 1,
                "page": 1,
                "pageSize": 50,
            }
        ),
        record_security_event=AsyncMock(),
    )
    identity = AsyncMock(return_value=user_admin)
    # Not monkeypatch: Starlette's State keeps attributes outside its
    # ``__dict__``, so pytest >= 9.2 records them as unset and its undo deletes
    # the application's real dependency.
    original = main.app.state.tables_dependencies
    main.app.state.tables_dependencies = TablesDependencies(db=db)
    monkeypatch.setattr(auth, "get_current_user", identity)
    yield SimpleNamespace(app=main.app, db=db, identity=identity)
    main.app.state.tables_dependencies = original


async def _get(api, path, **kwargs):
    # ASGITransport does not run the application's startup/shutdown lifespan.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app, client=("192.0.2.9", 12345)),
        base_url="http://tables.test",
    ) as client:
        return await client.get(path, **kwargs)


def _assert_no_reads(db):
    db.get_tables.assert_not_awaited()
    db.get_table_data.assert_not_awaited()
    db.get_table_schema.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _DENIED_PATHS)
async def test_unauthenticated_request_cannot_reach_allowlist_or_reads(
    tables_api, path
):
    tables_api.identity.side_effect = HTTPException(401, "Not authenticated")

    response = await _get(tables_api, path)

    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}
    tables_api.identity.assert_awaited_once()
    assert tables_api.identity.await_args.args[1] is tables_api.db
    _assert_no_reads(tables_api.db)
    tables_api.db.record_security_event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _DENIED_PATHS)
async def test_pending_admin_is_rejected_before_admin_gate_or_reads(
    tables_api, user_admin, path
):
    tables_api.identity.return_value = {**user_admin, "is_approved": False}

    response = await _get(tables_api, path)

    assert response.status_code == 403
    assert response.json() == {
        "detail": "Account pending approval. An administrator must approve your account."
    }
    _assert_no_reads(tables_api.db)
    tables_api.db.record_security_event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _DENIED_PATHS)
async def test_non_admin_denial_is_audited_before_allowlist_or_reads(
    tables_api, user_a, path
):
    tables_api.identity.return_value = user_a

    response = await _get(
        tables_api,
        path,
        headers={
            "X-Admin-View-As": "user",
            "X-Forwarded-For": "192.0.2.42, 192.0.2.43",
        },
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Admin access required"}
    _assert_no_reads(tables_api.db)
    tables_api.db.record_security_event.assert_awaited_once_with(
        event_type="admin_denied",
        user_id=str(user_a["id"]),
        auth_method="cookie",
        real_is_admin=False,
        view_as=False,
        resource_type="admin_endpoint",
        resource_id=path,
        method="GET",
        path=path,
        detail="Admin access required",
        client_ip="192.0.2.42",
    )


@pytest.mark.asyncio
async def test_audit_outage_cannot_mask_admin_denial(tables_api, user_a):
    tables_api.identity.return_value = user_a
    tables_api.db.record_security_event.side_effect = RuntimeError("audit unavailable")

    response = await _get(tables_api, "/api/tables/not_an_allowed_table")

    assert response.status_code == 403
    assert response.json() == {"detail": "Admin access required"}
    tables_api.db.record_security_event.assert_awaited_once()
    _assert_no_reads(tables_api.db)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _TABLE_PATHS)
@pytest.mark.parametrize("shadow", [False, True], ids=["admin", "shadow-admin"])
async def test_admin_and_shadow_admin_read_and_serialize_tables(
    tables_api, user_admin, path, shadow
):
    response = await _get(
        tables_api, path, headers={"X-Admin-View-As": "user"} if shadow else {}
    )

    assert response.status_code == 200
    columns = [{"name": "id", "type": "string", "nullable": False}]
    if path == "/api/tables":
        assert response.json() == [{"name": "users", "rowCount": 1}]
        tables_api.db.get_tables.assert_awaited_once_with()
    elif path.endswith("/schema"):
        assert response.json() == columns
        tables_api.db.get_table_schema.assert_awaited_once_with("users")
    else:
        assert response.json() == {
            "columns": columns,
            "rows": [{"id": str(user_admin["id"])}],
            "total": 1,
            "page": 1,
            "pageSize": 50,
        }
        tables_api.db.get_table_data.assert_awaited_once_with("users", 1, 50)
    tables_api.identity.assert_awaited_once()
    assert tables_api.identity.await_args.args[1] is tables_api.db
    tables_api.db.record_security_event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["", "/schema"])
async def test_authorized_unknown_table_returns_404_without_reads(tables_api, suffix):
    response = await _get(tables_api, f"/api/tables/not_an_allowed_table{suffix}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Table 'not_an_allowed_table' not found"}
    tables_api.identity.assert_awaited_once()
    _assert_no_reads(tables_api.db)
    tables_api.db.record_security_event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "page", "page_size"),
    [
        ({}, 1, 50),
        ({"page": -1, "pageSize": 500}, -1, 500),
        ({"page": 0, "pageSize": 1}, 0, 1),
        ({"page": 2, "pageSize": 17, "page_size": 999}, 2, 17),
        ({"page_size": 17}, 1, 50),
    ],
)
async def test_pagination_defaults_bounds_and_camel_case_alias(
    tables_api, params, page, page_size
):
    response = await _get(tables_api, "/api/tables/users", params=params)

    assert response.status_code == 200
    tables_api.db.get_table_data.assert_awaited_once_with("users", page, page_size)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "field", "error_type"),
    [
        ({"page": -2}, "page", "greater_than_equal"),
        ({"page": "bad"}, "page", "int_parsing"),
        ({"page": "1.5"}, "page", "int_parsing"),
        ({"pageSize": 0}, "pageSize", "greater_than_equal"),
        ({"pageSize": 501}, "pageSize", "less_than_equal"),
        ({"pageSize": "bad"}, "pageSize", "int_parsing"),
    ],
)
async def test_query_validation_precedes_handler_admission(
    tables_api, user_a, params, field, error_type
):
    tables_api.identity.return_value = user_a

    response = await _get(tables_api, "/api/tables/not_an_allowed_table", params=params)

    assert response.status_code == 422
    errors = response.json()["detail"]
    assert [(error["loc"], error["type"]) for error in errors] == [
        (["query", field], error_type)
    ]
    tables_api.identity.assert_not_awaited()
    _assert_no_reads(tables_api.db)
    tables_api.db.record_security_event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "method", "detail"),
    [
        ("/api/tables", "get_tables", "Internal server error"),
        ("/api/tables/users", "get_table_data", "synthetic table read failure"),
        (
            "/api/tables/users/schema",
            "get_table_schema",
            "synthetic table read failure",
        ),
    ],
)
async def test_database_errors_keep_endpoint_specific_http_handling(
    tables_api, path, method, detail
):
    getattr(tables_api.db, method).side_effect = RuntimeError(
        "synthetic table read failure"
    )

    response = await _get(tables_api, path)

    assert response.status_code == 500
    assert response.json() == {"detail": detail}
    getattr(tables_api.db, method).assert_awaited_once()
    tables_api.db.record_security_event.assert_not_awaited()


def test_table_openapi_preserves_operations_parameters_and_response_shapes(tables_api):
    paths = {
        "/api/tables": "list_tables_api_tables_get",
        "/api/tables/{table_name}": "get_table_data_api_tables__table_name__get",
        "/api/tables/{table_name}/schema": (
            "get_table_schema_api_tables__table_name__schema_get"
        ),
    }
    # Let FastAPI expand included routers, so the same assertions cover a move.
    # Assert only this domain's contract rather than storing a whole-app snapshot.
    document = get_openapi(
        title="Tables contract", version="1", routes=tables_api.app.routes
    )
    table_paths = {
        path: operations
        for path, operations in document["paths"].items()
        if path == "/api/tables" or path.startswith("/api/tables/")
    }
    assert set(table_paths) == paths.keys()
    object_schema = {"type": "object", "additionalProperties": True}
    path_parameter = {
        "name": "table_name",
        "in": "path",
        "required": True,
        "schema": {"type": "string", "title": "Table Name"},
    }
    for path, operation_id in paths.items():
        assert set(document["paths"][path]) == {"get"}
        operation = document["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert "requestBody" not in operation
        assert not operation.get("tags")
        schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
        schema_without_title = {
            key: value for key, value in schema.items() if key != "title"
        }
        is_data = path == "/api/tables/{table_name}"
        assert schema_without_title == (
            object_schema if is_data else {"type": "array", "items": object_schema}
        )
        if path == "/api/tables":
            assert operation.get("parameters", []) == []
            assert set(operation["responses"]) == {"200"}
        else:
            assert operation["parameters"][0] == path_parameter
            assert set(operation["responses"]) == {"200", "422"}
            assert operation["responses"]["422"]["content"]["application/json"][
                "schema"
            ] == {"$ref": "#/components/schemas/HTTPValidationError"}
        if path.endswith("/schema"):
            assert operation["parameters"] == [path_parameter]
    assert document["paths"]["/api/tables/{table_name}"]["get"]["parameters"][1:] == [
        {
            "name": "page",
            "in": "query",
            "required": False,
            "schema": {"type": "integer", "minimum": -1, "default": 1, "title": "Page"},
        },
        {
            "name": "pageSize",
            "in": "query",
            "required": False,
            "schema": {
                "type": "integer",
                "minimum": 1,
                "maximum": 500,
                "default": 50,
                "title": "Pagesize",
            },
        },
    ]
