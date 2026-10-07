"""Wire contracts for the connector (datasource) router.

New coverage: before R1.B03 these handlers lived in ``main.py`` and only their
*collaborators* were tested (``tests/test_datasource_access.py`` covers the
gates, ``tests/test_datasource_catalog.py`` the store query,
``tests/test_kb_datasource_api.py`` the normalizers). The behaviors pinned here
are the ones a handler move can quietly change:

* **which gate guards which route** — the six tiers are distinct, and a route
  that silently fell back to plain authentication would still look healthy in
  a happy-path test;
* **secrets never leave through a read** — every read path is shaped by
  ``redact_datasource``/``redact_datasources``, including the catalog's
  ``items`` and a job's resolved connectors;
* **route order** — ``/catalog`` and ``/eligible`` are literal segments that
  must win over ``/{datasource_id}``;
* **the MCP project scope refuses rather than widens** — on the catalog, the
  eligibility picker, create, and the scoped-mutation guard;
* **a connectivity probe reports, it does not raise** — a failing target is a
  200 with ``status: error``, while a disabled deployment is still a 403.
"""

import json
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


USER_ID = "00000000-0000-0000-0000-0000000000c1"
USER = {"id": USER_ID, "is_admin": False}
ADMIN = {"id": "00000000-0000-0000-0000-0000000000ad", "is_admin": True}
PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
OTHER_PROJECT_ID = "00000000-0000-0000-0000-0000000000b2"
DATASOURCE_ID = "00000000-0000-0000-0000-0000000000d1"
JOB_ID = "00000000-0000-0000-0000-0000000000j1".replace("j", "e")

SECRET_ROW = {
    "id": DATASOURCE_ID,
    "name": "prod-db",
    "type": "postgresql",
    "created_by": USER_ID,
    "credentials": {"password": "hunter2"},
    "connection_url": "postgresql://dbuser:hunter2@db.internal:5432/app",
}


def _scoped(project_id: str = PROJECT_ID) -> dict:
    """A project-scoped MCP principal (``scopes[0] == project:<uuid>``)."""
    return {"id": USER_ID, "is_admin": False, "scopes": [f"project:{project_id}"]}


def _store(**over):
    store = SimpleNamespace(
        list_datasources=AsyncMock(return_value=[dict(SECRET_ROW)]),
        list_datasource_catalog=AsyncMock(
            return_value={"items": [dict(SECRET_ROW)], "next_cursor": None}
        ),
        list_linkable_datasource_targets=AsyncMock(
            return_value={"items": [{"id": PROJECT_ID}], "next_cursor": None}
        ),
        list_eligible_datasources=AsyncMock(return_value=[dict(SECRET_ROW)]),
        list_datasource_projects=AsyncMock(return_value=[PROJECT_ID]),
        list_datasource_projects_bulk=AsyncMock(return_value={}),
        get_projects_for_user=AsyncMock(return_value=[{"id": PROJECT_ID}]),
        resolve_datasources_for_job=AsyncMock(return_value=[dict(SECRET_ROW)]),
        get_datasource=AsyncMock(return_value=dict(SECRET_ROW)),
        create_datasource=AsyncMock(return_value={"id": DATASOURCE_ID, "type": "kb"}),
        update_datasource=AsyncMock(return_value=True),
        update_datasource_with_policy=AsyncMock(
            return_value={"project_ids": [PROJECT_ID]}
        ),
        delete_datasource=AsyncMock(return_value=True),
        user_can_publish_datasource=AsyncMock(return_value=True),
        user_can_autonomous_send=AsyncMock(return_value=False),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _wire(
    *,
    store=None,
    user=None,
    datasource=None,
    gates=None,
    mcp_enabled=False,
    validate_mcp=None,
):
    from orchestrator.routers.datasources import DatasourcesDependencies as RouteDeps
    from orchestrator.routers.datasources import router
    from orchestrator.services.datasources import DatasourceDependencies as OpDeps
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    db = store or _store()
    caller = USER if user is None else user
    row = dict(SECRET_ROW) if datasource is None else datasource
    overrides = gates or {}

    async def approved(_request, _store):
        return caller

    async def project_member(_request, _store, project_id, **_kw):
        return caller, {"id": project_id}

    async def project_owner(_request, _store, project_id, **_kw):
        return caller, {"id": project_id}

    async def datasource_access(_request, _store, _datasource_id):
        return caller, row

    async def datasource_owner(_request, _store, _datasource_id):
        return caller, row

    async def job_access(_request, _store, job_id):
        return caller, {"id": job_id}

    tasks = KbDatasourceTaskRegistry()
    ops = OpDeps(
        store=db,
        vector_db=MagicMock(),
        knowledge_index=KnowledgeIndexDependencies(
            store=db,
            vector_db=MagicMock(),
            gitea_client=MagicMock(),
            logger=MagicMock(),
            tasks=tasks,
            inject_system_kb_embedding_profile=AsyncMock(return_value=None),
        ),
        mcp_datasources_enabled=lambda: mcp_enabled,
        validate_mcp_datasource=validate_mcp or (lambda _url, _creds: None),
    )
    deps = RouteDeps(
        store=db,
        operations=ops,
        require_approved_user=overrides.get("require_approved_user", approved),
        require_project_member=overrides.get("require_project_member", project_member),
        require_project_owner=overrides.get("require_project_owner", project_owner),
        require_datasource_access=overrides.get(
            "require_datasource_access", datasource_access
        ),
        require_datasource_owner=overrides.get(
            "require_datasource_owner", datasource_owner
        ),
        require_job_access=overrides.get("require_job_access", job_access),
    )
    app = mount_router(
        router, factories={"datasources_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(
        client=TestClient(app), store=db, ops=ops, tasks=tasks, row=row
    )


def _refusing(status: int, detail: str):
    async def gate(*_args, **_kwargs):
        raise HTTPException(status_code=status, detail=detail)

    return gate


def test_credentials_create_validates_env_and_returns_only_names():
    row = {
        **SECRET_ROW,
        "type": "credentials",
        "connection_url": None,
        "credentials": {"env_vars": {"API_KEY": "synthetic-key"}},
    }
    wire = _wire(store=_store(create_datasource=AsyncMock(return_value=row)))
    response = wire.client.post(
        "/api/datasources",
        json={
            "name": "My key",
            "type": "credentials",
            "credentials": row["credentials"],
        },
    )
    assert response.status_code == 200
    assert "synthetic-key" not in response.text
    assert response.json()["env_var_names"] == ["API_KEY"]
    assert (
        wire.store.create_datasource.await_args.kwargs["credentials"]
        == row["credentials"]
    )


def test_credentials_reject_reserved_env_before_storage():
    wire = _wire()
    response = wire.client.post(
        "/api/datasources",
        json={
            "name": "Bad key",
            "type": "credentials",
            "credentials": {"env_vars": {"PATH": "synthetic-key"}},
        },
    )
    assert response.status_code == 400
    assert "synthetic-key" not in response.text
    wire.store.create_datasource.assert_not_awaited()


def test_credentials_update_preserves_existing_fields():
    row = {
        **SECRET_ROW,
        "type": "credentials",
        "connection_url": None,
        "credentials": {"env_vars": {"USERNAME": "user", "PASSWORD": "previous"}},
    }
    wire = _wire(datasource=row)
    response = wire.client.put(
        f"/api/datasources/{DATASOURCE_ID}",
        json={
            "credentials": {"env_vars": {"PASSWORD": "replacement"}},
        },
    )
    assert response.status_code == 200
    call = wire.store.update_datasource.await_args
    assert call.kwargs["credentials"] == {
        "env_vars": {"USERNAME": "user", "PASSWORD": "replacement"}
    }


def test_credentials_delete_reports_active_attachment_conflict():
    from shared.credential_connectors import CredentialConnectorAttachedError

    wire = _wire(
        datasource={**SECRET_ROW, "type": "credentials"},
        store=_store(
            delete_datasource=AsyncMock(
                side_effect=CredentialConnectorAttachedError("End the session first")
            )
        ),
    )
    response = wire.client.delete(f"/api/datasources/{DATASOURCE_ID}")
    assert response.status_code == 409


# =============================================================================
# Gate tiers — one refusal per tier, so no route can quietly downgrade
# =============================================================================


@pytest.mark.parametrize(
    "gate,method,path",
    [
        ("require_approved_user", "post", "/api/datasources/ssh-keys/generate"),
        ("require_approved_user", "get", "/api/datasources"),
        ("require_approved_user", "get", "/api/datasources/catalog"),
        ("require_approved_user", "get", "/api/projects/linkable-datasource-targets"),
        ("require_approved_user", "get", "/api/datasources/eligible"),
        ("require_datasource_access", "get", f"/api/datasources/{DATASOURCE_ID}"),
        ("require_datasource_owner", "put", f"/api/datasources/{DATASOURCE_ID}"),
        ("require_datasource_owner", "delete", f"/api/datasources/{DATASOURCE_ID}"),
        ("require_job_access", "get", f"/api/jobs/{JOB_ID}/datasources"),
        (
            "require_datasource_access",
            "get",
            f"/api/datasources/{DATASOURCE_ID}/index-status",
        ),
        (
            "require_datasource_owner",
            "post",
            f"/api/datasources/{DATASOURCE_ID}/reindex",
        ),
        ("require_datasource_owner", "post", f"/api/datasources/{DATASOURCE_ID}/test"),
    ],
)
def test_each_route_refuses_when_its_gate_refuses(gate, method, path):
    wire = _wire(gates={gate: _refusing(403, "nope")})
    kwargs = {"json": {}} if method in ("post", "put") else {}
    response = getattr(wire.client, method)(path, **kwargs)
    assert response.status_code == 403
    assert response.json()["detail"] == "nope"


def test_catalog_requires_membership_of_the_filtered_project():
    wire = _wire(gates={"require_project_member": _refusing(403, "not a member")})
    assert wire.client.get("/api/datasources/catalog").status_code == 200
    denied = wire.client.get(f"/api/datasources/catalog?project_id={PROJECT_ID}")
    assert denied.status_code == 403
    assert denied.json()["detail"] == "not a member"


def test_eligible_requires_membership_of_every_supplied_project():
    wire = _wire(gates={"require_project_member": _refusing(403, "not a member")})
    assert wire.client.get("/api/datasources/eligible").status_code == 200
    denied = wire.client.get(f"/api/datasources/eligible?project_id={PROJECT_ID}")
    assert denied.status_code == 403


def test_linkable_targets_requires_ownership_only_when_a_connector_is_named():
    wire = _wire(gates={"require_datasource_owner": _refusing(403, "not the owner")})
    assert (
        wire.client.get("/api/projects/linkable-datasource-targets").status_code == 200
    )
    denied = wire.client.get(
        f"/api/projects/linkable-datasource-targets?datasource_id={DATASOURCE_ID}"
    )
    assert denied.status_code == 403


def test_create_authorizes_every_selected_project_as_owner():
    wire = _wire(gates={"require_project_owner": _refusing(403, "not the owner")})
    response = wire.client.post(
        "/api/datasources",
        json={
            "name": "n",
            "type": "generic",
            "scope_mode": "projects",
            "project_ids": [PROJECT_ID],
        },
    )
    assert response.status_code == 403
    wire.store.create_datasource.assert_not_awaited()


def test_create_validates_the_body_before_it_authenticates():
    """An unknown type is a 400 even for a caller the gate would reject."""
    wire = _wire(gates={"require_approved_user": _refusing(403, "nope")})
    response = wire.client.post("/api/datasources", json={"name": "n", "type": "nope"})
    assert response.status_code == 400
    assert "Invalid type 'nope'" in response.json()["detail"]


# =============================================================================
# Secret redaction — every read path
# =============================================================================


def test_list_strips_credentials_and_sanitizes_the_connection_url():
    wire = _wire(user=ADMIN)
    body = wire.client.get("/api/datasources").json()
    assert body == [
        {
            "id": DATASOURCE_ID,
            "name": "prod-db",
            "type": "postgresql",
            "created_by": USER_ID,
            "connection_url": "postgresql://db.internal:5432/app",
            "connection_url_redacted": True,
        }
    ]


def test_get_one_strips_credentials():
    wire = _wire()
    body = wire.client.get(f"/api/datasources/{DATASOURCE_ID}").json()
    assert "credentials" not in body
    assert "hunter2" not in body["connection_url"]
    # Creator/admin also learn which projects the connector is linked to.
    assert body["project_ids"] == [PROJECT_ID]


def test_catalog_items_are_redacted_and_the_envelope_survives():
    wire = _wire()
    body = wire.client.get("/api/datasources/catalog").json()
    assert body["next_cursor"] is None
    assert "credentials" not in body["items"][0]
    assert body["items"][0]["connection_url_redacted"] is True


def test_eligible_rows_are_redacted():
    wire = _wire()
    body = wire.client.get("/api/datasources/eligible").json()
    assert "credentials" not in body[0]


def test_job_datasources_are_redacted():
    wire = _wire()
    body = wire.client.get(f"/api/jobs/{JOB_ID}/datasources").json()
    assert "credentials" not in body[0]
    wire.store.resolve_datasources_for_job.assert_awaited_once_with(JOB_ID)


def test_a_non_owner_reader_does_not_learn_the_project_links():
    other = {"id": "00000000-0000-0000-0000-0000000000c9", "is_admin": False}
    wire = _wire(user=other)
    body = wire.client.get(f"/api/datasources/{DATASOURCE_ID}").json()
    assert "project_ids" not in body
    wire.store.list_datasource_projects.assert_not_awaited()


# =============================================================================
# Route order — the literal segments must win
# =============================================================================


def test_catalog_and_eligible_are_not_swallowed_by_the_id_route():
    wire = _wire()
    wire.client.get("/api/datasources/catalog")
    wire.client.get("/api/datasources/eligible")
    wire.store.list_datasource_catalog.assert_awaited_once()
    wire.store.list_eligible_datasources.assert_awaited_once()
    wire.store.get_datasource.assert_not_awaited()


# =============================================================================
# Cursor errors
# =============================================================================


def test_catalog_reports_an_unusable_cursor_as_400():
    from orchestrator.database.postgres import DatasourceCatalogCursorError

    store = _store(
        list_datasource_catalog=AsyncMock(
            side_effect=DatasourceCatalogCursorError("Invalid pagination cursor")
        )
    )
    wire = _wire(store=store)
    response = wire.client.get("/api/datasources/catalog?cursor=garbage")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid pagination cursor"


def test_catalog_reports_an_invalid_policy_filter_as_400():
    from orchestrator.database.postgres import DatasourcePolicyValidationError

    store = _store(
        list_datasource_catalog=AsyncMock(
            side_effect=DatasourcePolicyValidationError("bad scope_mode")
        )
    )
    wire = _wire(store=store)
    response = wire.client.get("/api/datasources/catalog?scope_mode=sideways")
    assert response.status_code == 400
    assert response.json()["detail"] == "bad scope_mode"


def test_linkable_targets_report_an_unusable_cursor_as_400():
    from orchestrator.database.postgres import DatasourceCatalogCursorError

    store = _store(
        list_linkable_datasource_targets=AsyncMock(
            side_effect=DatasourceCatalogCursorError("Invalid pagination cursor")
        )
    )
    wire = _wire(store=store)
    response = wire.client.get(
        "/api/projects/linkable-datasource-targets?cursor=garbage"
    )
    assert response.status_code == 400


# =============================================================================
# MCP project scope — refuse, never widen
# =============================================================================


def test_catalog_refuses_a_project_outside_the_token_scope():
    wire = _wire(user=_scoped())
    response = wire.client.get(
        f"/api/datasources/catalog?project_id={OTHER_PROJECT_ID}"
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Access denied by MCP token scope"


def test_catalog_restricts_the_visible_projects_to_the_token_scope():
    wire = _wire(user=_scoped())
    wire.client.get("/api/datasources/catalog")
    call = wire.store.list_datasource_catalog.await_args
    assert call.args[1] == [PROJECT_ID]
    assert call.kwargs["restrict_to_projects"] is True


def test_eligible_omission_narrows_to_the_scoped_project_rather_than_widening():
    wire = _wire(user=_scoped())
    wire.client.get("/api/datasources/eligible")
    assert wire.store.list_eligible_datasources.await_args.args[1] == [PROJECT_ID]


def test_eligible_refuses_a_project_outside_the_token_scope():
    wire = _wire(user=_scoped())
    response = wire.client.get(
        f"/api/datasources/eligible?project_id={OTHER_PROJECT_ID}"
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Access denied by MCP token scope"


def test_create_refuses_an_unscoped_connector_for_a_scoped_token():
    wire = _wire(user=_scoped())
    response = wire.client.post(
        "/api/datasources", json={"name": "n", "type": "generic"}
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "Access denied by MCP token scope"
    wire.store.create_datasource.assert_not_awaited()


def test_update_refuses_a_cross_scope_connector():
    """The row is linked to another project, so a scoped token cannot edit it."""
    store = _store(list_datasource_projects=AsyncMock(return_value=[OTHER_PROJECT_ID]))
    row = dict(SECRET_ROW, scope_mode="projects")
    wire = _wire(store=store, user=_scoped(), datasource=row)
    response = wire.client.put(f"/api/datasources/{DATASOURCE_ID}", json={"name": "x"})
    assert response.status_code == 403
    assert response.json()["detail"] == "Access denied by MCP token scope"
    wire.store.update_datasource.assert_not_awaited()


def test_delete_refuses_a_cross_scope_connector():
    store = _store(list_datasource_projects=AsyncMock(return_value=[OTHER_PROJECT_ID]))
    row = dict(SECRET_ROW, scope_mode="projects")
    wire = _wire(store=store, user=_scoped(), datasource=row)
    response = wire.client.delete(f"/api/datasources/{DATASOURCE_ID}")
    assert response.status_code == 403
    wire.store.delete_datasource.assert_not_awaited()


def test_delete_of_a_missing_row_is_404():
    store = _store(delete_datasource=AsyncMock(return_value=False))
    wire = _wire(store=store)
    response = wire.client.delete(f"/api/datasources/{DATASOURCE_ID}")
    assert response.status_code == 404
    assert response.json()["detail"] == f"Connector '{DATASOURCE_ID}' not found"


def test_delete_refuses_the_project_owned_knowledge_connector():
    row = dict(SECRET_ROW, type="kb", config={"native_project_id": PROJECT_ID})
    wire = _wire(datasource=row)
    response = wire.client.delete(f"/api/datasources/{DATASOURCE_ID}")
    assert response.status_code == 409
    assert (
        response.json()["detail"]
        == "The project knowledge connector is managed by its project"
    )


# =============================================================================
# Connectivity probe — reports, does not raise
# =============================================================================


def test_probe_of_an_unreachable_postgres_target_is_a_200_error_report():
    wire = _wire()
    with patch(
        "orchestrator.services.datasources.asyncpg.connect",
        AsyncMock(side_effect=OSError("connection refused")),
    ):
        body = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test").json()
    # The driver's own text can carry the connection URL (password included),
    # so it goes to the log and the client gets the code's own wording plus
    # the correlation id that finds that log line.
    assert body["status"] == "error"
    assert body["message"] == "PostgreSQL connection failed"
    assert "connection refused" not in json.dumps(body)
    assert re.fullmatch(r"[0-9a-f]{12}", body["error_ref"])


def test_probe_of_an_unknown_type_names_the_type():
    row = dict(SECRET_ROW, type="quantum")
    wire = _wire(datasource=row)
    body = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test").json()
    assert body == {"status": "error", "message": "Unknown connector type: quantum"}


def test_probe_of_a_generic_connector_says_there_is_nothing_to_test():
    row = dict(SECRET_ROW, type="generic")
    wire = _wire(datasource=row)
    body = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test").json()
    assert body == {
        "status": "ok",
        "message": "No connectivity test for generic connectors",
    }


def test_probe_of_an_mcp_connector_is_403_when_the_deployment_disables_them():
    row = dict(SECRET_ROW, type="mcp")
    wire = _wire(datasource=row, mcp_enabled=False)
    response = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test")
    assert response.status_code == 403
    assert response.json()["detail"] == "MCP connectors are disabled on this deployment"


def test_probe_of_an_ssh_repository_connector_reports_its_host_key(monkeypatch):
    """C1: no forge API takes a deploy key; Test reaches the SSH endpoint."""
    from orchestrator.services import workspace_ssh_connector
    from shared.runtime.utils.ssh_key import generate_ed25519_keypair

    host_key = " ".join(generate_ed25519_keypair().public_key.split()[:2])

    async def fetch(host, port):
        assert (host, port) == ("github.com", 22)
        return host_key

    monkeypatch.setattr(workspace_ssh_connector, "fetch_ssh_host_key", fetch)
    row = dict(
        SECRET_ROW,
        type="repository",
        connection_url="git@github.com:acme/widget.git",
        credentials={
            "auth_method": "ssh",
            "ssh_key": generate_ed25519_keypair().private_key,
        },
    )
    wire = _wire(datasource=row)
    body = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test").json()
    assert body["status"] == "ok"
    assert body["details"]["host_key"] == host_key
    assert "PRIVATE KEY" not in str(body)


def test_probe_of_an_unusable_ssh_repository_connector_is_an_error():
    row = dict(
        SECRET_ROW,
        type="repository",
        connection_url="git@github.com:acme/widget.git",
        credentials={"auth_method": "ssh", "ssh_key": "KEY"},
    )
    wire = _wire(datasource=row)
    body = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test").json()
    assert body["status"] == "error"
    assert "Invalid SSH key" in body["message"]


def test_probe_surfaces_a_gate_crash_as_a_500_without_its_message():
    """The gate resolves the connector *inside* the operation's try/except."""

    async def exploding_gate(*_args, **_kwargs):
        raise RuntimeError("pool exhausted")

    wire = _wire(gates={"require_datasource_owner": exploding_gate})
    response = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/test")
    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "pool exhausted" not in detail
    assert re.fullmatch(r"Connector test failed \(error_ref=[0-9a-f]{12}\)", detail)


# =============================================================================
# KB index surface on a connector
# =============================================================================


def test_index_status_refuses_a_non_kb_connector():
    wire = _wire()
    response = wire.client.get(f"/api/datasources/{DATASOURCE_ID}/index-status")
    assert response.status_code == 400
    assert response.json()["detail"] == "Connector is not an OKF Knowledge Base"


def test_reindex_refuses_a_non_kb_connector():
    wire = _wire()
    response = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/reindex")
    assert response.status_code == 400


def test_reindex_refuses_the_project_owned_knowledge_connector():
    row = dict(SECRET_ROW, type="kb", config={"native_project_id": PROJECT_ID})
    wire = _wire(datasource=row)
    response = wire.client.post(f"/api/datasources/{DATASOURCE_ID}/reindex")
    assert response.status_code == 400
    assert "reindex it from the project instead" in response.json()["detail"]


# =============================================================================
# SSH keypair generation
# =============================================================================


def test_ssh_keypair_is_generated_without_persisting_anything():
    wire = _wire()
    body = wire.client.post(
        "/api/datasources/ssh-keys/generate", json={"comment": "widget"}
    ).json()
    assert body["private_key"].startswith("-----BEGIN OPENSSH PRIVATE KEY-----")
    assert body["public_key"].startswith("ssh-ed25519 ")
    assert body["public_key"].rstrip().endswith("widget")
    wire.store.create_datasource.assert_not_awaited()
