"""forge field normalization on repository datasources."""

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.services.datasource_config import normalize_repository_config
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module


def test_explicit_forge_is_kept():
    out = normalize_repository_config(
        {"forge": "gitea"}, "https://git.example.com/acme/widget"
    )
    assert out["forge"] == "gitea"


def test_github_com_is_inferred():
    out = normalize_repository_config({}, "https://github.com/acme/widget")
    assert out["forge"] == "github"


def test_gitlab_com_is_inferred():
    out = normalize_repository_config(None, "https://gitlab.com/acme/widget")
    assert out["forge"] == "gitlab"


def test_self_hosted_host_cannot_be_inferred():
    """A self-hosted Gitea and a self-hosted GitLab look identical by URL."""
    with pytest.raises(HTTPException) as exc:
        normalize_repository_config({}, "https://git.example.com/acme/widget")
    assert exc.value.status_code == 400
    assert "forge" in str(exc.value.detail)


def test_unknown_forge_is_rejected():
    with pytest.raises(HTTPException) as exc:
        normalize_repository_config({"forge": "bitbucket"}, "https://bitbucket.org/a/b")
    assert exc.value.status_code == 400


def test_unrelated_config_keys_survive():
    out = normalize_repository_config(
        {"forge": "github", "path": "sub/dir"}, "https://github.com/a/b"
    )
    assert out["path"] == "sub/dir"


# =============================================================================
# Endpoint-level: exercises the real HTTP routes, not just the helper.
#
# The helper tests above call ``normalize_repository_config`` directly and
# would keep passing even if ``create_datasource``/``update_datasource`` never
# wired the helper into their type-dispatch chain — both routes end in a
# catch-all that 400s any non-empty config for a type it doesn't recognize,
# so a forgotten branch means every repository connector with a config 400s
# in production while these unit tests stay green. That gap is exactly the
# bug a previous pass shipped. These tests go through the real FastAPI routes
# with a real TestClient (no ``with`` block, so the ``lifespan`` startup that
# dials real Postgres/Keycloak/NATS never runs); only the DB layer and the
# auth resolvers are monkeypatched, so routing, Pydantic body validation, and
# the type-dispatch branching in orchestrator/services/datasources.py all run
# for real — including main's ``_datasources_dependencies`` factory, which is
# what wires the route to that service.
# =============================================================================


_OWNER_ID = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def app_client(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _OWNER_ID, "is_admin": False}),
    )
    return TestClient(main.app)


class TestCreateDatasourceRepositoryEndpoint:
    def test_post_repository_connector_with_forge_persists_config(
        self, app_client, monkeypatch
    ):
        from orchestrator import main

        async def fake_create_datasource(**kwargs):
            return {
                "id": "22222222-2222-2222-2222-222222222222",
                "name": kwargs["name"],
                "type": kwargs["ds_type"],
                "connection_url": kwargs["connection_url"],
                "description": kwargs["description"],
                "credentials": kwargs["credentials"],
                "job_id": kwargs["job_id"],
                "cli_hint": kwargs["cli_hint"],
                "default_branch": kwargs["default_branch"],
                "config": kwargs["config"],
                "created_by": kwargs["created_by"],
                "is_global": kwargs["is_global"],
                "read_only": kwargs["read_only"],
            }

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "create_datasource",
            AsyncMock(side_effect=fake_create_datasource),
        )

        response = app_client.post(
            "/api/datasources",
            json={
                "name": "widget",
                "type": "repository",
                "connection_url": "https://github.com/acme/widget",
                "config": {"forge": "github"},
            },
        )

        assert response.status_code < 300, response.text
        assert response.json()["config"]["forge"] == "github"

    def test_post_repository_connector_unresolvable_forge_stays_400(
        self, app_client, monkeypatch
    ):
        """Same route, self-hosted host, no explicit forge — must 400, not 500."""
        from orchestrator import main

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "create_datasource",
            AsyncMock(side_effect=AssertionError("must not persist a rejected config")),
        )

        response = app_client.post(
            "/api/datasources",
            json={
                "name": "widget",
                "type": "repository",
                "connection_url": "https://git.example.com/acme/widget",
                "config": {},
            },
        )

        assert response.status_code == 400, response.text
        assert "forge" in response.json()["detail"]


class TestUpdateDatasourceRepositoryEndpoint:
    def test_put_repository_connector_forge_change_persists(self, monkeypatch):
        from orchestrator import main

        existing_ds = {
            "id": "33333333-3333-3333-3333-333333333333",
            "name": "widget",
            "type": "repository",
            "connection_url": "https://git.example.com/acme/widget",
            "credentials": {},
            "config": {"forge": "gitea"},
            "is_global": False,
            "read_only": None,
        }
        monkeypatch.setattr(
            access_module,
            "require_datasource_owner",
            AsyncMock(return_value=({"id": _OWNER_ID, "is_admin": False}, existing_ds)),
        )
        captured: dict = {}

        async def fake_update_datasource(**kwargs):
            captured.update(kwargs)
            return True

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "update_datasource",
            AsyncMock(side_effect=fake_update_datasource),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_datasource",
            AsyncMock(return_value={**existing_ds, "config": {"forge": "gitlab"}}),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "list_datasource_projects",
            AsyncMock(return_value=[]),
        )
        client = TestClient(main.app)

        response = client.put(
            "/api/datasources/33333333-3333-3333-3333-333333333333",
            json={"config": {"forge": "gitlab"}},
        )

        assert response.status_code < 300, response.text
        assert captured["config"]["forge"] == "gitlab"

    def test_put_repository_connector_metadata_only_does_not_touch_config(
        self, monkeypatch
    ):
        """A rename-only PUT that never mentions ``config`` must not force
        forge re-validation. A self-hosted host can't be re-inferred from the
        URL, so without an ``is not None`` guard this would 400 on every
        unrelated update to a connector that already has a valid stored
        forge — see the sibling ``kb`` branch's ``if datasource_config is
        not None`` guard just above, which this mirrors.
        """
        from orchestrator import main

        existing_ds = {
            "id": "44444444-4444-4444-4444-444444444444",
            "name": "widget",
            "type": "repository",
            "connection_url": "https://git.example.com/acme/widget",
            "credentials": {},
            "config": {"forge": "gitea"},
            "is_global": False,
            "read_only": None,
        }
        monkeypatch.setattr(
            access_module,
            "require_datasource_owner",
            AsyncMock(return_value=({"id": _OWNER_ID, "is_admin": False}, existing_ds)),
        )
        captured: dict = {}

        async def fake_update_datasource(**kwargs):
            captured.update(kwargs)
            return True

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "update_datasource",
            AsyncMock(side_effect=fake_update_datasource),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_datasource",
            AsyncMock(return_value={**existing_ds, "name": "renamed-widget"}),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "list_datasource_projects",
            AsyncMock(return_value=[]),
        )
        client = TestClient(main.app)

        response = client.put(
            "/api/datasources/44444444-4444-4444-4444-444444444444",
            json={"name": "renamed-widget"},
        )

        assert response.status_code < 300, response.text
        assert captured["config"] is None
