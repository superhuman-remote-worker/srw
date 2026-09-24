"""Repository connector probe: who the token is, what it may do, no token exposure."""

import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from orchestrator.routers.datasources import (
    test_datasource as probe_datasource_endpoint,
)
from shared.runtime.services import forge
from shared.runtime.services.forge import ForgeError, ForgeRepo, probe_repository_access
from orchestrator.services import datasource_config as datasource_config_module


def _route_deps(row: dict):
    """The connector router's collaborators with the owner gate resolved to ``row``.

    Replaces the old ``patch("orchestrator.main.require_datasource_owner")``:
    the route now reads that gate off the dependency dataclass, so patching a
    ``main`` global would no longer intercept it. The gate is still *bound*
    rather than awaited up front, which is what keeps an unexpected resolution
    failure inside the probe's own try/except.
    """
    from orchestrator.services.deployment_gates import (
        mcp_datasources_enabled as _mcp_datasources_enabled,
    )
    from orchestrator.routers.datasources import DatasourcesDependencies
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    store = MagicMock()
    return DatasourcesDependencies(
        store=store,
        operations=DatasourceDependencies(
            store=store,
            vector_db=MagicMock(),
            knowledge_index=KnowledgeIndexDependencies(
                store=store,
                vector_db=MagicMock(),
                gitea_client=MagicMock(),
                logger=MagicMock(),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
            mcp_datasources_enabled=_mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
        ),
        require_datasource_owner=AsyncMock(return_value=({}, row)),
    )


TOKEN = "ghp_secretsecretsecret"


def _target(
    forge_name: str, url_owner: str = "acme", repo: str = "widgets"
) -> ForgeRepo:
    api_base = {
        "github": "https://api.github.com",
        "gitea": "https://git.example.test/api/v1",
        "gitlab": "https://gitlab.example.test/api/v4",
    }[forge_name]
    return ForgeRepo(
        forge=forge_name, api_base=api_base, owner=url_owner, repo=repo, token=TOKEN
    )


def _transport(
    *,
    user: dict | int,
    repo: dict | int,
    user_headers: dict[str, str] | None = None,
    seen: list | None = None,
) -> httpx.MockTransport:
    """Answer ``/user`` and the repository read; ints are bare status codes."""

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        if request.url.path.endswith("/user"):
            if isinstance(user, int):
                return httpx.Response(user, json={"message": "Bad credentials"})
            return httpx.Response(200, json=user, headers=user_headers or {})
        if isinstance(repo, int):
            return httpx.Response(repo, json={"message": "Not Found"})
        return httpx.Response(200, json=repo)

    return httpx.MockTransport(handler)


class TestProbeGitHub:
    @pytest.mark.asyncio
    async def test_classic_admin_token_is_named_and_warned(self, monkeypatch):
        seen: list[httpx.Request] = []
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "owner", "id": 7},
                user_headers={"X-OAuth-Scopes": "public_repo, read:org"},
                repo={
                    "default_branch": "main",
                    "permissions": {"admin": True, "push": True, "pull": True},
                },
                seen=seen,
            ),
        )

        facts = await probe_repository_access(_target("github"))

        assert facts["principal"] == "owner"
        assert facts["token_class"] == "classic"
        assert facts["scopes"] == ["public_repo", "read:org"]
        assert facts["is_admin"] is True and facts["can_write"] is True
        assert facts["default_branch"] == "main"
        assert any("admin bypass" in w for w in facts["warnings"])
        assert seen[0].headers["Authorization"] == f"Bearer {TOKEN}"
        assert seen[0].url == "https://api.github.com/user"
        assert seen[1].url == "https://api.github.com/repos/acme/widgets"

    @pytest.mark.asyncio
    async def test_fine_grained_write_token_has_no_warnings(self, monkeypatch):
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "srw-bot", "id": 99},
                repo={
                    "default_branch": "main",
                    "permissions": {"admin": False, "push": True, "pull": True},
                },
            ),
        )

        facts = await probe_repository_access(_target("github"))

        assert facts["token_class"] == "fine-grained"
        assert facts["scopes"] is None
        assert facts["is_admin"] is False and facts["can_write"] is True
        assert facts["warnings"] == []

    @pytest.mark.asyncio
    async def test_classic_repo_scope_is_warned_as_account_wide(self, monkeypatch):
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "srw-bot", "id": 99},
                user_headers={"X-OAuth-Scopes": "repo"},
                repo={"default_branch": "main", "permissions": {"push": True}},
            ),
        )

        facts = await probe_repository_access(_target("github"))

        assert facts["token_class"] == "classic"
        assert any("'repo' scope" in w for w in facts["warnings"])


class TestProbeOtherForges:
    @pytest.mark.asyncio
    async def test_gitea_uses_token_scheme_and_unknown_class(self, monkeypatch):
        seen: list[httpx.Request] = []
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "bot", "id": 3},
                repo={"default_branch": "main", "permissions": {"push": True}},
                seen=seen,
            ),
        )

        facts = await probe_repository_access(_target("gitea"))

        assert facts["token_class"] == "unknown"
        assert facts["can_write"] is True and facts["is_admin"] is False
        assert seen[0].headers["Authorization"] == f"token {TOKEN}"
        assert seen[1].url == "https://git.example.test/api/v1/repos/acme/widgets"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "level, can_write, is_admin",
        [(20, False, False), (30, True, False), (40, True, True)],
    )
    async def test_gitlab_maps_access_levels(
        self, monkeypatch, level, can_write, is_admin
    ):
        seen: list[httpx.Request] = []
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"username": "bot", "id": 3},
                repo={
                    "default_branch": "main",
                    "permissions": {
                        "project_access": {"access_level": level},
                        "group_access": None,
                    },
                },
                seen=seen,
            ),
        )

        facts = await probe_repository_access(_target("gitlab"))

        assert facts["principal"] == "bot"
        assert facts["can_write"] is can_write and facts["is_admin"] is is_admin
        assert seen[0].headers["PRIVATE-TOKEN"] == TOKEN
        assert (
            seen[1].url == "https://gitlab.example.test/api/v4/projects/acme%2Fwidgets"
        )


class TestProbeFailures:
    @pytest.mark.asyncio
    async def test_rejected_token_never_echoes_it(self, monkeypatch):
        monkeypatch.setattr(forge, "_transport", _transport(user=401, repo={}))

        with pytest.raises(ForgeError, match="rejected the token") as exc:
            await probe_repository_access(_target("github"))
        assert TOKEN not in str(exc.value)

    @pytest.mark.asyncio
    async def test_invisible_repository_is_a_404_explanation(self, monkeypatch):
        monkeypatch.setattr(
            forge, "_transport", _transport(user={"login": "bot", "id": 1}, repo=404)
        )

        with pytest.raises(ForgeError, match="not found on github, or the token"):
            await probe_repository_access(_target("github"))

    @pytest.mark.asyncio
    async def test_unreachable_forge_is_a_forge_error(self, monkeypatch):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom", request=request)

        monkeypatch.setattr(forge, "_transport", httpx.MockTransport(handler))

        with pytest.raises(ForgeError, match="Could not reach github"):
            await probe_repository_access(_target("github"))

    @pytest.mark.asyncio
    async def test_missing_token_is_refused_before_any_request(self):
        target = ForgeRepo(
            forge="github",
            api_base="https://api.github.com",
            owner="a",
            repo="b",
            token="",
        )
        with pytest.raises(ForgeError, match="no token"):
            await probe_repository_access(target)


class TestRepositoryConnectorEndpoint:
    """``POST /api/datasources/{id}/test`` for ``repository`` rows."""

    DS_ID = "11111111-2222-3333-4444-555555555555"

    def _row(self, **overrides) -> dict:
        row = {
            "id": self.DS_ID,
            "type": "repository",
            "connection_url": "https://github.com/acme/widgets.git",
            "config": json.dumps({"forge": "github"}),
            "credentials": {"token": TOKEN},
            "default_branch": "develop",
            "read_only": False,
        }
        row.update(overrides)
        return row

    @pytest.mark.asyncio
    async def test_token_connector_reports_principal_and_branch(self, monkeypatch):
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "srw-bot", "id": 5},
                repo={"default_branch": "main", "permissions": {"push": True}},
            ),
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "ok"
        assert "Authenticated as srw-bot (fine-grained token)" in result["message"]
        assert "write access to acme/widgets" in result["message"]
        assert "default branch main (connector targets develop)" in result["message"]
        assert "WARNING" not in result["message"]
        assert result["details"]["principal"] == "srw-bot"
        assert TOKEN not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_read_only_mismatch_and_admin_are_warned(self, monkeypatch):
        monkeypatch.setattr(
            forge,
            "_transport",
            _transport(
                user={"login": "owner", "id": 5},
                user_headers={"X-OAuth-Scopes": "repo"},
                repo={
                    "default_branch": "main",
                    "permissions": {"admin": True, "push": False},
                },
            ),
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "ok"
        message = result["message"]
        assert "WARNING" in message
        assert "admin bypass" in message
        assert (
            "cannot push to acme/widgets but the connector is not marked read-only"
            in message
        )
        assert "'repo' scope" in message
        assert len(result["details"]["warnings"]) == 3

    @pytest.mark.asyncio
    async def test_rejected_token_is_an_error_result(self, monkeypatch):
        monkeypatch.setattr(forge, "_transport", _transport(user=401, repo={}))
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(self._row())
        )

        assert result["status"] == "error"
        assert "rejected the token" in result["message"]
        assert TOKEN not in result["message"]

    @pytest.mark.asyncio
    async def test_ssh_connector_is_not_probed(self):
        row = self._row(credentials={"ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----"})
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result["status"] == "ok"
        assert "No API probe for SSH-key" in result["message"]

    @pytest.mark.asyncio
    async def test_self_hosted_without_forge_is_an_error_not_a_guess(self):
        row = self._row(
            connection_url="https://git.example.test/acme/widgets.git", config="{}"
        )
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result["status"] == "error"
        assert "forge" in result["message"]

    @pytest.mark.asyncio
    async def test_generic_connector_keeps_its_no_test_message(self):
        row = self._row(type="generic", credentials={})
        result = await probe_datasource_endpoint(
            object(), self.DS_ID, dependencies=_route_deps(row)
        )

        assert result == {
            "status": "ok",
            "message": "No connectivity test for generic connectors",
        }
