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
from jsonschema import Draft202012Validator

from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.matrix import capability_matrix
from orchestrator.services.manifest_connectors import (
    DESCRIPTION,
    DISPLAY_NAME,
    connector_document,
    connector_endpoint,
    connector_resource_name,
    connector_scope,
    declared_config,
)
from shared.connectors.builtin import (
    BUILTIN_SPECS,
    DATASOURCE_SPECS,
    MCP_REMOTE_DRIVER,
    MCP_SPEC,
)
from shared.connectors.platform import (
    PROJECT_CLOUD,
    PROJECT_KB,
    PROJECT_KINDS,
    REFUSAL_ACTIONS,
    USER_CLOUD_ROOT,
    USER_KINDS,
    managed_key,
    managed_key_for,
    managed_kind,
    managed_refusal,
    native_kb_project,
    native_project,
    native_project_sql,
    native_user,
    native_user_sql,
    platform_owned,
    platform_owned_sql,
    project_cloud_key,
    project_kb_key,
    project_managed_keys,
    user_managed_keys,
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
    "s3cret-cmd",
    "s3cret-path",
    "s3cret-query",
    "s3cret-hint",
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
    """What persist_connector_resource writes for ``row``, minus the store."""
    driver = REGISTRY.for_type(row["type"])
    scope = connector_scope(row, list(links))
    assert scope is not None
    name = driver.resource_driver(row["credentials"])
    return connector_document(
        row,
        scope=scope,
        driver=name,
        credential_config=driver.credential_config(row["credentials"]),
        existing=existing,
        declared=declared_config(REGISTRY.get(name).spec),
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
            cli_hint="GITHUB_TOKEN=s3cret-hint make test",
            default_branch="main",
        ),
        "srw.repository/v1",
        {
            "forge": "github",
            "endpoint": "https://git.example",
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
            "endpoint": "ssh://git.example",
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
            "endpoint": "https://git.example",
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
        {"endpoint": "postgresql://db.internal:5432"},
    ),
    (
        "neo4j",
        _row(
            "neo4j",
            connection_url="bolt://graph.internal:7687",
            credentials={"username": "neo4j", "password": "s3cret-pass"},
        ),
        "srw.neo4j/v1",
        {"endpoint": "bolt://graph.internal:7687"},
    ),
    (
        "mongodb",
        _row(
            "mongodb",
            connection_url="mongodb://reader:s3cret-pass@mongo.internal/app",
        ),
        "srw.mongodb/v1",
        {"endpoint": "mongodb://mongo.internal"},
    ),
    (
        "webdav",
        _row(
            "webdav",
            connection_url="https://cloud.example/remote.php/dav/files/me/",
            credentials={"username": "me", "password": "s3cret-pass"},
        ),
        "srw.webdav/v1",
        {"endpoint": "https://cloud.example"},
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
            "endpoint": "https://mcp.example",
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
            "endpoint": "https://mcp.example",
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
                "command": "s3cret-cmd-runner --token s3cret-cmd",
                "args": ["server", "--key", "s3cret-arg"],
                "env": {"API_KEY": "s3cret-env"},
            },
        ),
        "srw.mcp/v1",
        {"transport": "stdio"},
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

    @pytest.mark.parametrize(
        ("label", "row", "driver", "config"),
        TYPE_CASES,
        ids=[case[0] for case in TYPE_CASES],
    )
    def test_each_stored_config_validates_against_its_drivers_schema(
        self, label, row, driver, config
    ):
        document = _document(row)
        schema = REGISTRY.get(document["spec"]["driver"]).spec.config_schema
        errors = list(
            Draft202012Validator(dict(schema)).iter_errors(document["spec"]["config"])
        )
        assert errors == [], [error.message for error in errors]

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


class TestPlatformKinds:
    """Every platform-owned kind: whose own connector a key names."""

    @pytest.mark.parametrize(
        ("key", "kind", "project", "user"),
        [
            ("project-kb:" + PROJECT, PROJECT_KB, PROJECT, None),
            ("project-cloud:" + PROJECT, PROJECT_CLOUD, PROJECT, None),
            (
                "project-cloud-checkout:" + PROJECT,
                "project-cloud-checkout",
                PROJECT,
                None,
            ),
            ("user-cloud-root:" + OWNER, USER_CLOUD_ROOT, None, OWNER),
            ("cloud-outbox:" + OWNER, "cloud-outbox", None, OWNER),
            # A kind SRW does not know yet names neither.
            ("future-thing:" + PROJECT, "future-thing", None, None),
        ],
    )
    def test_a_key_names_its_kind_and_its_owner(self, key, kind, project, user):
        row = _row("cloud_folder", created_by=None, managed_key=key)
        assert managed_kind(row) == kind
        assert native_project(row) == project
        assert native_user(row) == user
        # Only the knowledge base is a knowledge base.
        assert native_kb_project(row) == (project if kind == PROJECT_KB else None)

    @pytest.mark.parametrize(
        "key",
        [
            "project-cloud:not-a-uuid",
            "project-cloud:",
            "project-cloud",
            "user-cloud-root:x",
        ],
    )
    def test_a_malformed_key_names_nobody_but_stays_owned(self, key):
        row = _row("cloud_folder", managed_key=key)
        assert platform_owned(row) == key
        assert native_project(row) is None
        assert native_user(row) is None

    def test_the_older_marker_is_a_project_knowledge_base(self):
        row = _row("kb", config={"native_project_id": PROJECT.upper()})
        assert managed_kind(row) == PROJECT_KB
        assert native_project(row) == PROJECT
        assert native_user(row) is None
        # The marker only means something on a knowledge base.
        other = _row("cloud_folder", config={"native_project_id": PROJECT})
        assert managed_kind(other) is None
        assert native_project(other) is None

    @pytest.mark.parametrize("row", [None, {}, _row("postgresql")])
    def test_ordinary_rows_have_no_kind(self, row):
        assert managed_kind(row) is None
        assert native_project(row) is None
        assert native_user(row) is None

    def test_keys_are_canonical_and_kinds_are_closed(self):
        assert project_cloud_key(PROJECT.upper()) == "project-cloud:" + PROJECT
        assert managed_key(USER_CLOUD_ROOT, OWNER) == "user-cloud-root:" + OWNER
        with pytest.raises(ValueError):
            managed_key("user-cloud", OWNER)
        with pytest.raises(ValueError):
            managed_key(PROJECT_CLOUD, "not-a-uuid")
        assert set(project_managed_keys(PROJECT)) == {
            f"{kind}:{PROJECT}" for kind in PROJECT_KINDS
        }
        assert set(user_managed_keys(OWNER)) == {
            f"{kind}:{OWNER}" for kind in USER_KINDS
        }
        assert not PROJECT_KINDS & USER_KINDS
        shape = re.compile(
            r"^[a-z][a-z0-9-]{0,62}:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        for key in project_managed_keys(PROJECT) + user_managed_keys(OWNER):
            assert shape.fullmatch(key)

    def test_the_sql_twins_name_every_kind(self):
        project_sql = native_project_sql("d")
        assert project_sql.startswith("(CASE WHEN d.managed_key IS NOT NULL")
        for kind in PROJECT_KINDS:
            assert f"'{kind}'" in project_sql
        assert "WHEN d.type = 'kb' THEN lower(NULLIF(" in project_sql
        user_sql = native_user_sql("d")
        for kind in USER_KINDS:
            assert f"'{kind}'" in user_sql
        assert "d.type" not in user_sql
        assert native_project_sql().startswith("(CASE WHEN managed_key IS NOT NULL")


class TestManagedRefusal:
    KB_KEY = _row("kb", managed_key=project_kb_key(PROJECT))
    KB_MARKER = _row("kb", config={"native_project_id": PROJECT})

    @pytest.mark.parametrize("row", [KB_KEY, KB_MARKER], ids=["key", "marker"])
    def test_a_knowledge_base_keeps_its_wording(self, row):
        # Pinned by the API goldens (tests/fixtures/connector_goldens/api.json).
        assert managed_refusal(row, "policy") == (
            "The native project knowledge connector policy is managed by its project"
        )
        assert managed_refusal(row, "delete") == (
            "The project knowledge connector is managed by its project"
        )
        assert managed_refusal(row, "link") == (
            "The native knowledge connector cannot be linked to another project"
        )
        assert managed_refusal(row, "unlink") == (
            "The native knowledge connector link is managed by its project"
        )

    def test_a_project_cloud_folder_says_what_it_is(self):
        row = _row("cloud_folder", managed_key=project_cloud_key(PROJECT))
        assert managed_refusal(row, "delete") == (
            "The project cloud folder connector is managed by its project"
        )
        assert managed_refusal(row, "edit") == (
            "The project cloud folder connector is managed by its project "
            "and cannot be edited"
        )
        assert managed_refusal(row, "link") == (
            "The project cloud folder connector cannot be linked to another project"
        )
        for action in REFUSAL_ACTIONS:
            assert "knowledge" not in managed_refusal(row, action)

    def test_a_users_folder_is_srws_for_its_user(self):
        row = _row("cloud_folder", managed_key="user-cloud-root:" + OWNER)
        assert managed_refusal(row, "delete") == (
            "The personal cloud folder connector is managed by SRW for its user"
        )
        assert managed_refusal(row, "link") == (
            "The personal cloud folder connector cannot be linked to a project"
        )

    def test_an_unknown_action_is_a_bug(self):
        with pytest.raises(ValueError):
            managed_refusal(self.KB_KEY, "rename")


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


# =============================================================================
# Nothing secret-looking reaches a resource: the exact inputs a review found
# =============================================================================


@pytest.mark.parametrize(
    ("url", "endpoint"),
    [
        # A token in the path.
        ("https://mcp.zapier.com/api/mcp/s/s3cret-path/mcp", "https://mcp.zapier.com"),
        # Query keys no allowlist knows.
        (
            "https://api.example.com/v1?code=s3cret-query&k=s3cret-query",
            "https://api.example.com",
        ),
        # A JDBC URL's user and password.
        (
            "jdbc:postgresql://db.example:5432/app?user=admin&password=s3cret-pass",
            "jdbc:postgresql://db.example:5432",
        ),
        # Userinfo and a fragment.
        (
            "https://bob:s3cret-pass@dav.example:8443/remote.php#s3cret-query",
            "https://dav.example:8443",
        ),
        ("postgresql://app:s3cret-pass@[::1]:5432/app", "postgresql://[::1]:5432"),
        # Nothing to cut a host from.
        ("git@github.com:acme/s3cret-path.git", None),
        ("host=db user=admin password=s3cret-pass", None),
        ("mongodb://a:1,b:2/s3cret-path", None),
        (None, None),
    ],
)
def test_only_the_scheme_host_and_port_of_a_url_are_kept(url, endpoint):
    assert connector_endpoint(url) == endpoint


@pytest.mark.parametrize(
    "row",
    [
        _row(
            "mcp",
            connection_url="https://mcp.zapier.com/api/mcp/s/s3cret-path/mcp",
            credentials={"transport": "http"},
        ),
        _row(
            "webdav",
            connection_url="https://cloud.example/remote.php/dav/files/s3cret-path/",
        ),
        _row(
            "postgresql",
            connection_url=(
                "jdbc:postgresql://db.example:5432/app?user=s3cret-query"
                "&password=s3cret-pass"
            ),
        ),
        _row(
            "generic",
            connection_url="https://api.example.com/v1?code=s3cret-query&k=s3cret-query",
            cli_hint="curl -H 'Authorization: Bearer s3cret-hint' https://api.example.com",
        ),
        _row(
            "mcp",
            credentials={
                "transport": "stdio",
                "command": "/usr/bin/env API_KEY=s3cret-cmd npx server",
                "args": ["--token", "s3cret-arg"],
            },
        ),
    ],
    ids=[
        "mcp-path-token",
        "webdav-path",
        "jdbc-user",
        "generic-query-and-hint",
        "stdio",
    ],
)
def test_no_secret_looking_part_reaches_a_document(row):
    text = json.dumps(_document(row))
    assert not [secret for secret in SECRETS if secret in text]
    assert "cli_hint" not in text and "command" not in text and "args" not in text


class TestTheMcpDrivers:
    def test_both_names_are_registered_and_one_owns_the_type(self):
        assert REGISTRY.get(MCP_SPEC.name).spec is MCP_SPEC
        remote = REGISTRY.get(MCP_REMOTE_DRIVER)
        assert remote is not None and remote.spec.legacy_type == "mcp"
        assert REGISTRY.for_type("mcp").spec is MCP_SPEC
        assert remote.serves_stored_type is False
        # Same implementation: either answers a row of either transport.
        assert type(remote) is type(REGISTRY.for_type("mcp"))

    def test_the_matrix_lists_the_remote_driver_after_the_stdio_one(self):
        drivers = capability_matrix(REGISTRY)["drivers"]
        names = [driver["name"] for driver in drivers]
        assert names == [spec.name for spec in BUILTIN_SPECS]
        assert names.index(MCP_REMOTE_DRIVER) == names.index(MCP_SPEC.name) + 1
        # Clients tell the two apart: only one owns the stored type.
        owners = {
            driver["name"]: driver["serves_stored_type"]
            for driver in drivers
            if driver["legacy_type"] == "mcp"
        }
        assert owners == {MCP_SPEC.name: True, MCP_REMOTE_DRIVER: False}
        assert all(
            driver["serves_stored_type"] is (driver["legacy_type"] is not None)
            for driver in drivers
            if driver["name"] != MCP_REMOTE_DRIVER
        )

    def test_a_row_and_a_payload_entry_name_the_same_driver(self):
        from shared.connectors.builtin import driver_spec_for_row

        for credentials, name in (
            ({"transport": "stdio"}, MCP_SPEC.name),
            ({"transport": "http"}, MCP_REMOTE_DRIVER),
        ):
            row = {"type": "mcp", "credentials": credentials}
            assert driver_spec_for_row(row).name == name
            assert REGISTRY.for_type("mcp").resource_driver(credentials) == name
        assert driver_spec_for_row({"type": "postgresql"}).name == "srw.postgresql/v1"

    @pytest.mark.parametrize(
        ("credentials", "driver"),
        [
            ({"transport": "stdio", "command": "x"}, "srw.mcp/v1"),
            ({"transport": "http"}, MCP_REMOTE_DRIVER),
            ({"transport": "SSE"}, MCP_REMOTE_DRIVER),
            ({}, MCP_REMOTE_DRIVER),
        ],
    )
    def test_a_row_names_the_driver_of_its_transport(self, credentials, driver):
        assert REGISTRY.for_type("mcp").resource_driver(credentials) == driver
