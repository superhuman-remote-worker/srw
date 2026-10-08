"""Datasources become Connectors, slice D3a: the row-to-resource mapping.

Pure tests of ``orchestrator.services.manifest_connectors`` and of the
platform-owned marker (``shared.connectors.platform``).  What the database
does with them (the migrations, the startup backfill, the write-through and
its atomicity) is in ``tests/test_manifest_connectors_real_postgres.py``.
"""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.manifest_connectors import (
    DESCRIPTION,
    DISPLAY_NAME,
    connector_document,
    connector_resource_name,
    connector_scope,
)
from shared.connectors.builtin import DATASOURCE_SPECS, MCP_REMOTE_DRIVER
from shared.connectors.platform import (
    managed_key_for,
    native_kb_project,
    platform_owned,
    platform_owned_sql,
    project_kb_key,
)
from shared.manifests import preview_documents, validate_documents

OWNER = "00000000-0000-4000-8000-0000000000c1"
PROJECT = "00000000-0000-4000-8000-0000000000b1"
OTHER_PROJECT = "00000000-0000-4000-8000-0000000000b2"
NAME = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
REGISTRY = builtin_connector_drivers()

#: Every value here is secret; none may reach a Connector document.
SECRETS = (
    "s3cret-env",
    "s3cret-token",
    "s3cret-key",
    "s3cret-pass",
    "s3cret-header",
    "s3cret-arg",
    "s3cret-file",
)


def _row(ds_type: str, index: int = 1, **over) -> dict:
    row = {
        "id": UUID(f"00000000-0000-4000-8000-{index:012x}"),
        "name": f"{ds_type} connector",
        "description": None,
        "type": ds_type,
        "connection_url": None,
        "credentials": {},
        "config": {},
        "cli_hint": None,
        "default_branch": None,
        "created_by": OWNER,
        "is_global": False,
        "read_only": None,
        "job_id": None,
        "managed_key": None,
    }
    row.update(over)
    return row


def _document(row: dict, links=(), existing=None) -> dict:
    driver = REGISTRY.for_type(row["type"])
    scope = connector_scope(row, list(links))
    assert scope is not None
    return connector_document(
        row,
        scope=scope,
        driver=driver.resource_driver(row["credentials"]),
        credential_config=driver.credential_config(row["credentials"]),
        existing=existing,
    )


#: (label, row, driver, expected spec.config)
TYPE_CASES = [
    (
        "generic",
        _row("generic", credentials={"env_vars": {"API_KEY": "s3cret-env"}}),
        "srw.generic/v1",
        {},
    ),
    (
        "credentials",
        _row("credentials", credentials={"env_vars": {"TOKEN": "s3cret-env"}}),
        "srw.credentials/v1",
        {},
    ),
    (
        "repository-token",
        _row(
            "repository",
            connection_url="https://bot:s3cret-token@git.example/acme/app.git",
            credentials={"auth_method": "token", "token": "s3cret-token"},
            config={"forge": "github"},
            cli_hint="make test",
            default_branch="main",
        ),
        "srw.repository/v1",
        {
            "forge": "github",
            "connection_url": "https://git.example/acme/app.git",
            "connection_url_redacted": True,
            "cli_hint": "make test",
            "default_branch": "main",
            "auth_method": "token",
        },
    ),
    (
        "repository-ssh",
        _row(
            "repository",
            connection_url="ssh://git@git.example/acme/app.git",
            credentials={"auth_method": "ssh", "ssh_key": "s3cret-key"},
            config={"known_hosts": "git.example ssh-ed25519 AAAA"},
        ),
        "srw.repository/v1",
        {
            "known_hosts": "git.example ssh-ed25519 AAAA",
            "connection_url": "ssh://git.example/acme/app.git",
            "connection_url_redacted": True,
            "auth_method": "ssh",
        },
    ),
    (
        "kb",
        _row(
            "kb",
            connection_url="https://git.example/acme/kb.git",
            credentials={"auth_method": "token", "token": "s3cret-token"},
            config={"root_path": "knowledge"},
            read_only=True,
        ),
        "srw.kb/v1",
        {
            "root_path": "knowledge",
            "connection_url": "https://git.example/acme/kb.git",
            "auth_method": "token",
        },
    ),
    (
        "postgresql",
        _row(
            "postgresql",
            connection_url="postgresql://app:s3cret-pass@db.internal:5432/app",
        ),
        "srw.postgresql/v1",
        {
            "connection_url": "postgresql://db.internal:5432/app",
            "connection_url_redacted": True,
        },
    ),
    (
        "neo4j",
        _row(
            "neo4j",
            connection_url="bolt://graph.internal:7687",
            credentials={"username": "neo4j", "password": "s3cret-pass"},
        ),
        "srw.neo4j/v1",
        {"connection_url": "bolt://graph.internal:7687"},
    ),
    (
        "mongodb",
        _row(
            "mongodb",
            connection_url="mongodb://reader:s3cret-pass@mongo.internal/app",
        ),
        "srw.mongodb/v1",
        {
            "connection_url": "mongodb://mongo.internal/app",
            "connection_url_redacted": True,
        },
    ),
    (
        "webdav",
        _row(
            "webdav",
            connection_url="https://cloud.example/remote.php/dav/files/me/",
            credentials={"username": "me", "password": "s3cret-pass"},
        ),
        "srw.webdav/v1",
        {"connection_url": "https://cloud.example/remote.php/dav/files/me/"},
    ),
    (
        "email",
        _row(
            "email",
            credentials={
                "backend": "imap_smtp",
                "username": "agent@example.com",
                "password": "s3cret-pass",
                "imap": {"host": "imap.example.com", "port": 993, "security": "ssl"},
                "smtp": {"host": "smtp.example.com", "port": 587},
            },
            config={"access": "draft"},
        ),
        "srw.email/v1",
        {
            "access": "draft",
            "backend": "imap_smtp",
            "username": "agent@example.com",
            "imap": {"host": "imap.example.com", "port": 993, "security": "ssl"},
            "smtp": {"host": "smtp.example.com", "port": 587},
        },
    ),
    (
        "mcp-remote-headers",
        _row(
            "mcp",
            connection_url="https://mcp.example/mcp",
            credentials={
                "transport": "http",
                "auth": {
                    "type": "headers",
                    "headers": {"X-Tenant": "s3cret-header", "Authorization": "x"},
                },
            },
        ),
        MCP_REMOTE_DRIVER,
        {
            "connection_url": "https://mcp.example/mcp",
            "transport": "http",
            "auth_type": "headers",
            "header_names": ["Authorization", "X-Tenant"],
        },
    ),
    (
        "mcp-remote-bearer",
        _row(
            "mcp",
            connection_url="https://mcp.example/sse",
            credentials={
                "transport": "SSE",
                "auth": {"type": "bearer", "token": "s3cret-token"},
            },
        ),
        MCP_REMOTE_DRIVER,
        {
            "connection_url": "https://mcp.example/sse",
            "transport": "sse",
            "auth_type": "bearer",
        },
    ),
    (
        "mcp-stdio",
        _row(
            "mcp",
            credentials={
                "transport": "stdio",
                "command": "npx",
                "args": ["server", "--key", "s3cret-arg"],
                "env": {"API_KEY": "s3cret-env"},
            },
        ),
        "srw.mcp/v1",
        {"transport": "stdio", "command": "npx"},
    ),
    (
        "kubeconfig",
        _row(
            "kubeconfig",
            credentials={
                "files": [
                    {
                        "name": "prod.yaml",
                        "contents": "s3cret-file",
                        "target_path": "~/.kube/configs/prod.yaml",
                        "mode": "0600",
                    }
                ]
            },
        ),
        "srw.kubeconfig/v1",
        {
            "files": [
                {
                    "name": "prod.yaml",
                    "target_path": "~/.kube/configs/prod.yaml",
                    "mode": "0600",
                }
            ]
        },
    ),
    (
        "ssh_key",
        _row(
            "ssh_key",
            credentials={
                "files": [
                    {
                        "name": "deploy",
                        "contents": "s3cret-key",
                        "target_path": "~/.ssh/deploy",
                        "mode": "0600",
                    }
                ]
            },
            config={"host": "bastion.example", "user": "ops"},
        ),
        "srw.ssh-key/v1",
        {
            "host": "bastion.example",
            "user": "ops",
            "files": [
                {"name": "deploy", "target_path": "~/.ssh/deploy", "mode": "0600"}
            ],
        },
    ),
    (
        "generic_file",
        _row(
            "generic_file",
            credentials={
                "files": [
                    {
                        "name": "file-0",
                        "contents": "s3cret-file",
                        "target_path": "~/.config/tool/token",
                        "mode": "0600",
                        "env_var": "TOOL_TOKEN_FILE",
                    }
                ]
            },
        ),
        "srw.generic-file/v1",
        {
            "files": [
                {
                    "name": "file-0",
                    "target_path": "~/.config/tool/token",
                    "mode": "0600",
                    "env_var": "TOOL_TOKEN_FILE",
                }
            ]
        },
    ),
]


class TestTheMapping:
    def test_every_stored_type_has_a_case(self):
        covered = {row["type"] for _label, row, _driver, _config in TYPE_CASES}
        assert covered == {spec.legacy_type for spec in DATASOURCE_SPECS}

    @pytest.mark.parametrize(
        ("label", "row", "driver", "config"),
        TYPE_CASES,
        ids=[case[0] for case in TYPE_CASES],
    )
    def test_each_type_maps_to_its_driver_and_config(self, label, row, driver, config):
        document = _document(row)
        assert document["kind"] == "Connector"
        assert document["spec"]["driver"] == driver
        assert document["spec"]["config"] == config
        # The secrets stay on the row in D3a: no references, no values.
        assert "credentials" not in document["spec"]
        text = json.dumps(document)
        assert not [secret for secret in SECRETS if secret in text]
        validate_documents([document])
        preview_documents([document])

    @pytest.mark.parametrize("read_only", [True, False, None])
    def test_access_is_read_only_only_for_a_read_only_row(self, read_only):
        document = _document(_row("postgresql", read_only=read_only))
        if read_only:
            assert document["spec"]["access"] == "ReadOnly"
        else:
            assert "access" not in document["spec"]

    def test_names_and_descriptions_are_annotations(self):
        row = _row("postgresql", name="Prod DB (EU) ü", description="Orders")
        document = _document(row)
        annotations = document["metadata"]["annotations"]
        assert annotations[DISPLAY_NAME] == "Prod DB (EU) ü"
        assert annotations[DESCRIPTION] == "Orders"
        assert document["metadata"]["name"] == "prod-db-eu-000000000000"

    def test_the_native_marker_is_not_driver_config(self):
        row = _row(
            "kb",
            created_by=None,
            read_only=True,
            config={"root_path": "knowledge", "native_project_id": PROJECT},
        )
        assert _document(row)["spec"]["config"] == {"root_path": "knowledge"}

    def test_an_existing_document_keeps_its_name_and_other_metadata(self):
        row = _row("postgresql", name="renamed", description=None)
        before = _document(_row("postgresql", name="first", description="old"))
        before["metadata"]["labels"] = {"team": "data"}
        after = _document(row, existing=before)
        assert after["metadata"]["name"] == before["metadata"]["name"]
        assert after["metadata"]["labels"] == {"team": "data"}
        assert after["metadata"]["annotations"][DISPLAY_NAME] == "renamed"
        assert DESCRIPTION not in after["metadata"]["annotations"]


class TestNames:
    def test_one_owner_may_name_a_postgres_and_a_neo4j_connector_prod(self):
        # Legal per (name, type, owner) on the row; one scope for both here.
        postgres = _row(
            "postgresql", name="prod", id=UUID("aaaaaaaa-1111-4000-8000-000000000001")
        )
        neo4j = _row(
            "neo4j", name="prod", id=UUID("bbbbbbbb-2222-4000-8000-000000000002")
        )
        first, second = _document(postgres), _document(neo4j)
        assert first["metadata"]["scope"] == second["metadata"]["scope"]
        assert first["metadata"]["name"] == "prod-aaaaaaaa1111"
        assert second["metadata"]["name"] == "prod-bbbbbbbb2222"

    @pytest.mark.parametrize(
        "name", ["", "---", "x" * 200, "Ünïcödé only", "a_b c.d", "-lead-"]
    )
    def test_every_free_text_name_yields_a_legal_manifest_name(self, name):
        row = _row("generic", name=name)
        for full_id in (False, True):
            result = connector_resource_name(row, full_id=full_id)
            assert NAME.fullmatch(result), result
            assert len(result) <= 63

    def test_the_full_id_fallback_carries_all_32_hex_digits(self):
        row = _row("generic", name="prod")
        assert connector_resource_name(row, full_id=True).endswith(
            str(row["id"]).replace("-", "")
        )


class TestScope:
    def test_an_owned_row_lives_in_its_creators_account(self):
        assert connector_scope(_row("postgresql"), [PROJECT, OTHER_PROJECT]) == {
            "kind": "Account",
            "name": OWNER,
        }

    def test_a_native_kb_lives_in_its_project_even_with_a_creator(self):
        row = _row("kb", config={"native_project_id": PROJECT})
        assert connector_scope(row, [PROJECT]) == {"kind": "Project", "name": PROJECT}

    def test_the_managed_key_names_the_native_project(self):
        row = _row("kb", created_by=None, managed_key=project_kb_key(PROJECT))
        assert connector_scope(row, []) == {"kind": "Project", "name": PROJECT}

    def test_an_ownerless_row_with_one_link_lives_in_that_project(self):
        assert connector_scope(_row("postgresql", created_by=None), [PROJECT]) == {
            "kind": "Project",
            "name": PROJECT,
        }

    @pytest.mark.parametrize("links", [[], [PROJECT, OTHER_PROJECT]])
    def test_other_ownerless_rows_stay_on_the_legacy_path(self, links):
        assert connector_scope(_row("postgresql", created_by=None), links) is None

    def test_a_malformed_native_marker_names_no_project(self):
        row = _row("kb", created_by=None, config={"native_project_id": "nope"})
        assert connector_scope(row, []) is None


class TestPlatformOwned:
    def test_the_managed_key_is_the_reason(self):
        row = _row("webdav", managed_key="project-cloud:" + PROJECT)
        assert platform_owned(row) == "project-cloud:" + PROJECT
        assert native_kb_project(row) is None
        assert managed_key_for(row) == "project-cloud:" + PROJECT

    def test_the_native_marker_still_counts_for_one_release(self):
        row = _row("kb", config={"native_project_id": PROJECT})
        assert platform_owned(row) == project_kb_key(PROJECT)
        assert native_kb_project(row) == PROJECT
        assert managed_key_for(row) == project_kb_key(PROJECT)

    def test_a_malformed_marker_is_refused_but_never_stored(self):
        row = _row("kb", config={"native_project_id": "not-a-uuid"})
        assert platform_owned(row) == "project-kb:not-a-uuid"
        assert native_kb_project(row) is None
        assert managed_key_for(row) is None

    @pytest.mark.parametrize(
        "row",
        [
            None,
            {},
            _row("postgresql"),
            _row("kb"),
            _row("kb", config={"native_project_id": ""}),
            # The marker only means something on a knowledge base.
            _row("repository", config={"native_project_id": PROJECT}),
        ],
    )
    def test_ordinary_rows_are_not_platform_owned(self, row):
        assert platform_owned(row) is None
        assert native_kb_project(row) is None

    def test_the_sql_twin_reads_the_key_and_the_marker(self):
        assert platform_owned_sql("d") == (
            "(d.managed_key IS NOT NULL OR (d.type = 'kb' AND "
            "NULLIF(d.config->>'native_project_id', '') IS NOT NULL))"
        )
        assert platform_owned_sql().startswith("(managed_key IS NOT NULL")

    def test_the_key_has_the_shape_the_column_checks(self):
        shape = re.compile(
            r"^[a-z][a-z0-9-]{0,62}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        assert shape.fullmatch(project_kb_key(PROJECT.upper()))


# =============================================================================
# The guards read the marker (managed_key) and the older native marker alike
# =============================================================================

MANAGED_ONLY = _row(
    "kb",
    config={"root_path": "knowledge"},
    managed_key=project_kb_key(PROJECT),
    read_only=True,
    is_global=True,
)
MARKER_ONLY = _row(
    "kb", config={"native_project_id": PROJECT}, read_only=True, is_global=True
)


def _datasource_dependencies(store):
    from orchestrator.services.datasources import DatasourceDependencies

    return DatasourceDependencies(
        store=store,
        vector_db=MagicMock(),
        knowledge_index=MagicMock(),
        mcp_datasources_enabled=lambda: True,
        mcp_stdio_enabled=lambda: True,
        validate_mcp_datasource=lambda _url, _creds: None,
        connector_drivers=REGISTRY,
    )


@pytest.mark.parametrize("row", [MANAGED_ONLY, MARKER_ONLY], ids=["key", "marker"])
class TestPlatformOwnedGuards:
    USER = {"id": OWNER, "is_admin": True}

    @pytest.mark.asyncio
    async def test_delete_is_refused(self, row):
        from orchestrator.services.datasources import delete_datasource

        store = SimpleNamespace(delete_datasource=AsyncMock())
        with pytest.raises(HTTPException) as refused:
            await delete_datasource(
                user=self.USER,
                datasource=dict(row),
                datasource_id=str(row["id"]),
                dependencies=_datasource_dependencies(store),
            )
        assert refused.value.status_code == 409
        assert refused.value.detail == (
            "The project knowledge connector is managed by its project"
        )
        store.delete_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_policy_update_is_refused(self, row):
        from orchestrator.schemas.datasources import DatasourceUpdate
        from orchestrator.services.datasources import update_datasource

        store = SimpleNamespace(update_datasource_with_policy=AsyncMock())
        with pytest.raises(HTTPException) as refused:
            await update_datasource(
                request=MagicMock(),
                datasource_id=str(row["id"]),
                body=DatasourceUpdate(auto_attach=False, policy_revision=1),
                user=self.USER,
                existing_ds=dict(row),
                require_project_owner=AsyncMock(),
                dependencies=_datasource_dependencies(store),
            )
        assert refused.value.status_code == 409
        assert "managed by its project" in refused.value.detail
        store.update_datasource_with_policy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_link_and_unlink_are_refused(self, row):
        from orchestrator.services.projects import (
            link_datasource_to_project,
            resolve_datasource_unlink,
        )

        store = SimpleNamespace(
            get_datasource=AsyncMock(return_value=dict(row)),
            link_datasource_to_project=AsyncMock(),
        )
        dependencies = SimpleNamespace(store=store)
        with pytest.raises(HTTPException) as linked:
            await link_datasource_to_project(
                OTHER_PROJECT,
                str(row["id"]),
                None,
                user=self.USER,
                dependencies=dependencies,
            )
        assert linked.value.status_code == 409
        with pytest.raises(HTTPException) as unlinked:
            await resolve_datasource_unlink(
                PROJECT, str(row["id"]), user=self.USER, dependencies=dependencies
            )
        assert unlinked.value.status_code == 409
        store.link_datasource_to_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reindex_as_an_external_source_is_refused(self, row):
        with pytest.raises(HTTPException) as refused:
            await REGISTRY.for_type("kb").reindex(
                dict(row), full=False, knowledge_index=MagicMock()
            )
        assert refused.value.status_code == 400
        assert "reindex it from the project instead" in refused.value.detail


def test_the_policy_reads_the_native_project_from_the_managed_key():
    from orchestrator.services.datasource_policy import _native_project_id

    assert _native_project_id(MANAGED_ONLY) == PROJECT
    assert _native_project_id(MARKER_ONLY) == PROJECT
    assert _native_project_id(_row("postgresql")) is None
