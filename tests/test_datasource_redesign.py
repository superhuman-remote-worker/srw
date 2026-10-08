"""Tests for the datasource redesign: KB templates and payload building.

Covers the three datasource categories (generic, repository, managed
connectors). Managed connectors are tool-backed in both access modes; the
former CLI mode (env vars in the agent process) is gone.

The KB notes are the real ones: ``knowledge_projection`` asks each
connector's driver for its note. The helpers below keep this module's old
call shapes over that one entry point. The payload builder is replicated
here (the original lives in the connector drivers now).
"""

import json

import pytest

from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.knowledge_projection import build_datasource_note_content

_DRIVERS = builtin_connector_drivers()


def _build_datasource_note_content(ds: dict) -> str:
    return build_datasource_note_content(ds, drivers=_DRIVERS)


def _note(ds_type: str, name: str, desc: str, row: dict | None = None) -> str:
    return _build_datasource_note_content(
        {**(row or {}), "type": ds_type, "name": name, "description": desc}
    )


def _build_generic_note(name: str, desc: str, ds: dict) -> str:
    return _note("generic", name, desc, ds)


def _build_repository_note(name: str, desc: str, ds: dict) -> str:
    return _note("repository", name, desc, ds)


def _build_managed_readwrite_note(name: str, desc: str, ds_type: str) -> str:
    return _note(ds_type, name, desc, {"project_read_only": False})


def _build_managed_readonly_note(name: str, desc: str, ds_type: str) -> str:
    return _note(ds_type, name, desc, {"project_read_only": True})


def _build_webdav_note(name: str, desc: str, is_read_only: bool) -> str:
    return _note("webdav", name, desc, {"project_read_only": is_read_only})


# =============================================================================
# Replicated payload builder. Keep in sync with the original.
# =============================================================================


def _build_datasources_payload(resolved_ds):
    if not resolved_ds:
        return None
    managed_types = {"postgresql", "neo4j", "mongodb", "webdav"}
    payload = []
    for ds in resolved_ds:
        creds = ds.get("credentials") or {}
        if isinstance(creds, str):
            try:
                creds = json.loads(creds)
            except (json.JSONDecodeError, ValueError):
                creds = {}
        is_read_only = ds.get("project_read_only", False)
        ds_type = ds["type"]
        if ds_type in managed_types and is_read_only:
            creds = {}
        entry = {
            "type": ds_type,
            "name": ds["name"],
            "description": ds.get("description"),
            "connection_url": ds.get("connection_url"),
            "credentials": creds,
            "project_read_only": is_read_only,
        }
        if ds.get("cli_hint"):
            entry["cli_hint"] = ds["cli_hint"]
        if ds.get("default_branch"):
            entry["default_branch"] = ds["default_branch"]
        payload.append(entry)
    return payload or None


# =============================================================================
# KB Note Content Builders
# =============================================================================


class TestGenericNote:
    """KB note content for generic datasources."""

    def test_basic_fields(self):
        ds = {
            "name": "Production DB",
            "description": "Analytics database with user events",
            "connection_url": "postgresql://host:5432/analytics",
            "cli_hint": "psql $DATABASE_URL",
            "credentials": {
                "env_vars": {"DATABASE_URL": "secret", "DB_SCHEMA": "public"}
            },
        }
        content = _build_generic_note("Production DB", ds["description"], ds)
        assert "## Connector: Production DB" in content
        assert "Analytics database with user events" in content
        assert "postgresql://host:5432/analytics" in content
        assert "`psql $DATABASE_URL`" in content
        assert "`DATABASE_URL`" in content
        assert "`DB_SCHEMA`" in content

    def test_no_credentials_leak(self):
        """Env var values must never appear in KB content."""
        ds = {
            "name": "Secret DB",
            "credentials": {"env_vars": {"API_KEY": "sk-super-secret-key-12345"}},
        }
        content = _build_generic_note("Secret DB", "", ds)
        assert "sk-super-secret-key-12345" not in content
        assert "`API_KEY`" in content

    def test_no_url_no_cli(self):
        ds = {
            "name": "Minimal",
            "credentials": {"env_vars": {"TOKEN": "abc"}},
        }
        content = _build_generic_note("Minimal", "", ds)
        assert "## Connector: Minimal" in content
        assert "`TOKEN`" in content

    def test_empty_env_vars(self):
        ds = {"name": "Empty", "credentials": {}}
        content = _build_generic_note("Empty", "Some desc", ds)
        assert "## Connector: Empty" in content
        assert "Some desc" in content
        assert "Environment Variables" not in content

    def test_credentials_as_json_string(self):
        ds = {
            "name": "StringCreds",
            "credentials": json.dumps({"env_vars": {"MY_VAR": "val"}}),
        }
        content = _build_generic_note("StringCreds", "", ds)
        assert "`MY_VAR`" in content


class TestRepositoryNote:
    """KB note content for repository datasources."""

    def test_basic_repo(self):
        ds = {"name": "Frontend App", "default_branch": "develop"}
        content = _build_repository_note("Frontend App", "React SPA", ds)
        assert "## Repository: Frontend App" in content
        assert "React SPA" in content
        assert "./repos/frontend-app/" in content
        assert "git is pre-authenticated" in content
        assert "`develop`" in content

    def test_slug_generation(self):
        ds = {"name": "My Awesome Repo!!!"}
        content = _build_repository_note("My Awesome Repo!!!", "", ds)
        assert "./repos/my-awesome-repo/" in content

    def test_no_branch(self):
        ds = {"name": "Repo"}
        content = _build_repository_note("Repo", "", ds)
        assert "Default branch" not in content

    def test_git_commands_present(self):
        ds = {"name": "Test"}
        content = _build_repository_note("Test", "", ds)
        assert "git status" in content
        assert "git pull" in content
        assert "git push" in content
        assert "No login or credential setup required" in content


class TestManagedReadWriteNote:
    """KB note for managed connectors in read-write mode: read and write tools."""

    def test_postgresql(self):
        content = _build_managed_readwrite_note("Analytics", "Big data", "postgresql")
        assert "**Access:** read-write (tools)" in content
        assert "`sql_query`" in content
        assert "`sql_execute`" in content
        assert "psql" not in content
        assert "PGHOST" not in content

    def test_neo4j(self):
        content = _build_managed_readwrite_note("Graph DB", "", "neo4j")
        assert "`cypher_execute`" in content
        assert "cypher-shell" not in content

    def test_mongodb(self):
        content = _build_managed_readwrite_note("Docs DB", "", "mongodb")
        assert "`mongo_insert`" in content
        assert "`mongo_update`" in content
        assert "mongosh" not in content


class TestManagedReadOnlyNote:
    """KB note for managed connectors in read-only (tools) mode."""

    def test_postgresql_readonly(self):
        content = _build_managed_readonly_note("Analytics", "", "postgresql")
        assert "**Access:** read-only (tools)" in content
        assert "`sql_query`" in content
        assert "`sql_schema`" in content
        assert "No CLI access" in content

    def test_neo4j_readonly(self):
        content = _build_managed_readonly_note("Graph", "", "neo4j")
        assert "`cypher_query`" in content

    def test_mongodb_readonly(self):
        content = _build_managed_readonly_note("Docs", "", "mongodb")
        assert "`mongo_query`" in content
        assert "`mongo_aggregate`" in content


class TestWebdavNote:
    """KB note for WebDAV (always tools)."""

    def test_readwrite(self):
        content = _build_webdav_note("Files", "Shared storage", False)
        assert "**Access:** read-write" in content
        assert "`webdav_write`" in content
        assert "`webdav_delete`" in content

    def test_readonly(self):
        content = _build_webdav_note("Files", "", True)
        assert "**Access:** read-only" in content
        assert "webdav_write" not in content
        assert "webdav_delete" not in content
        assert "`webdav_list`" in content
        assert "`webdav_read`" in content


class TestBuildDatasourceNoteContent:
    """Top-level dispatcher that selects the right template per type/mode."""

    def test_dispatches_generic(self):
        ds = {"type": "generic", "name": "Test", "credentials": {}}
        assert "## Connector: Test" in _build_datasource_note_content(ds)

    def test_dispatches_repository(self):
        ds = {"type": "repository", "name": "Repo"}
        assert "## Repository: Repo" in _build_datasource_note_content(ds)

    def test_dispatches_managed_readwrite(self):
        ds = {"type": "postgresql", "name": "DB", "project_read_only": False}
        assert "read-write (tools)" in _build_datasource_note_content(ds)

    def test_dispatches_managed_readonly(self):
        ds = {"type": "postgresql", "name": "DB", "project_read_only": True}
        assert "read-only (tools)" in _build_datasource_note_content(ds)

    def test_dispatches_webdav(self):
        ds = {"type": "webdav", "name": "DAV", "project_read_only": False}
        assert "webdav" in _build_datasource_note_content(ds)

    def test_unknown_type_fallback(self):
        ds = {"type": "redis", "name": "Cache", "description": "Redis cache"}
        content = _build_datasource_note_content(ds)
        assert "Cache" in content

    def test_default_readwrite_when_no_flag(self):
        """Missing project_read_only defaults to False (read-write)."""
        ds = {"type": "neo4j", "name": "Graph"}
        assert "read-write (tools)" in _build_datasource_note_content(ds)


# =============================================================================
# Datasource Payload Builder
# =============================================================================


class TestBuildDatasourcesPayload:
    """Tests for _build_datasources_payload — sent to the agent at job start."""

    def test_empty_returns_none(self):
        assert _build_datasources_payload([]) is None
        assert _build_datasources_payload(None) is None

    def test_basic_payload(self):
        ds = [
            {
                "type": "postgresql",
                "name": "DB",
                "description": "Test",
                "connection_url": "postgres://host/db",
                "credentials": '{"password": "secret"}',
                "project_read_only": False,
            }
        ]
        result = _build_datasources_payload(ds)
        assert len(result) == 1
        assert result[0]["type"] == "postgresql"
        assert result[0]["credentials"]["password"] == "secret"

    def test_readonly_managed_withholds_credentials(self):
        """Read-only managed connectors must NOT receive credentials."""
        for ds_type in ("postgresql", "neo4j", "mongodb", "webdav"):
            ds = [
                {
                    "type": ds_type,
                    "name": f"RO {ds_type}",
                    "connection_url": "some://url",
                    "credentials": '{"password": "secret"}',
                    "project_read_only": True,
                }
            ]
            result = _build_datasources_payload(ds)
            assert result[0]["credentials"] == {}, (
                f"{ds_type} should withhold creds in read-only"
            )

    def test_readwrite_managed_keeps_credentials(self):
        ds = [
            {
                "type": "postgresql",
                "name": "RW DB",
                "connection_url": "postgres://host/db",
                "credentials": {"password": "secret"},
                "project_read_only": False,
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["credentials"]["password"] == "secret"

    def test_generic_always_keeps_credentials(self):
        ds = [
            {
                "type": "generic",
                "name": "API",
                "connection_url": None,
                "credentials": {"env_vars": {"API_KEY": "abc"}},
                "project_read_only": False,
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["credentials"]["env_vars"]["API_KEY"] == "abc"

    def test_repository_always_keeps_credentials(self):
        ds = [
            {
                "type": "repository",
                "name": "Repo",
                "connection_url": "https://github.com/org/repo.git",
                "credentials": {"auth_method": "token", "token": "ghp_xxx"},
                "project_read_only": False,
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["credentials"]["token"] == "ghp_xxx"

    def test_cli_hint_included_when_present(self):
        ds = [
            {
                "type": "generic",
                "name": "DB",
                "connection_url": None,
                "credentials": {},
                "project_read_only": False,
                "cli_hint": "psql $DB_URL",
                "default_branch": None,
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["cli_hint"] == "psql $DB_URL"
        assert "default_branch" not in result[0]

    def test_default_branch_included_when_present(self):
        ds = [
            {
                "type": "repository",
                "name": "Repo",
                "connection_url": "url",
                "credentials": {},
                "project_read_only": False,
                "cli_hint": None,
                "default_branch": "develop",
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["default_branch"] == "develop"
        assert "cli_hint" not in result[0]

    def test_invalid_json_credentials_handled(self):
        ds = [
            {
                "type": "postgresql",
                "name": "DB",
                "connection_url": "postgres://host/db",
                "credentials": "not-valid-json",
                "project_read_only": False,
            }
        ]
        result = _build_datasources_payload(ds)
        assert result[0]["credentials"] == {}

    def test_multiple_datasources(self):
        ds = [
            {
                "type": "generic",
                "name": "A",
                "connection_url": None,
                "credentials": {},
                "project_read_only": False,
            },
            {
                "type": "repository",
                "name": "B",
                "connection_url": "url",
                "credentials": {},
                "project_read_only": False,
            },
            {
                "type": "postgresql",
                "name": "C",
                "connection_url": "pg://h/d",
                "credentials": {},
                "project_read_only": True,
            },
        ]
        result = _build_datasources_payload(ds)
        assert len(result) == 3
        assert [r["type"] for r in result] == ["generic", "repository", "postgresql"]


class TestRenderInstructionContentNoCliMode:
    """The CLI mode is gone. For one release the loader still offers its
    template names, always empty, so a stored template that guards a block
    with them (an admin override on a self-hosted install) renders it exactly
    as it always did at runtime: not at all."""

    #: The block the bundled prompts carried, in the same shape.
    DEAD_BLOCK = (
        "</constraints>\n\n"
        "{% if cli_datasources and has_shell -%}\n"
        "<datasource_access>\n"
        "Use `run_command`.\n"
        '{% if has_cli_datasource("postgresql") -%}\n'
        "- PostgreSQL: `psql`\n"
        "{% endif -%}\n"
        '{% if has_cli_datasource("neo4j") -%}\n'
        "- Neo4j: `cypher-shell`\n"
        "{% endif -%}\n"
        "</datasource_access>\n"
        "{% endif -%}\n\n"
        "<phase_model>\n"
    )

    @pytest.mark.parametrize("tools", [["run_command"], ["read_file"]])
    def test_the_dead_block_renders_nothing(self, tools):
        from shared.runtime.core.loader import render_instruction_content

        assert (
            render_instruction_content(self.DEAD_BLOCK, tools)
            == "</constraints>\n\n<phase_model>\n"
        )

    def test_the_names_are_empty_even_outside_the_guard(self):
        from shared.runtime.core.loader import render_instruction_content

        template = (
            '{% if has_cli_datasource("postgresql") %}PG{% endif %}'
            "[{{ cli_datasources | length }}]"
        )
        assert render_instruction_content(template, ["run_command"]) == "[0]"

    def test_bundled_config_no_longer_mentions_it(self):
        from pathlib import Path

        config = Path(__file__).parents[1] / "config"
        offenders = []
        for path in config.rglob("*"):
            if not path.is_file():
                continue
            try:
                text = path.read_text()
            except UnicodeDecodeError:
                continue
            if "cli_datasource" in text:
                offenders.append(str(path.relative_to(config)))
        assert offenders == []


class TestRenderInstructionContentProtectedCloud:
    """Tests for the F-C1 protected-cloud honesty block (Task 15)."""

    PROTECTED_BLOCK_TEMPLATE = (
        "Workspace:\n"
        "- Your workspace is your persistent working area.\n"
        "{% if protected_cloud %}\n"
        "Protected cloud mode:\n"
        "- The cloud folder (workspace/cloud) is in PROTECTED mode: everything "
        "you write there is STAGED for the user's review — nothing is saved to "
        "the cloud or visible to anyone else until the user applies it in the "
        "review panel.\n"
        '- Never say a cloud file is "saved", "uploaded", or "shared". Say it '
        'is "staged for your review".\n'
        "- When a piece of work is ready, tell the user so they can open the "
        'review panel ("Cloud changes" in the session header) and apply it.\n'
        "{% endif %}\n"
        "Conversation style:\n"
    )

    def test_protected_block_rendered_when_flag_true(self):
        from shared.runtime.core.loader import render_instruction_content

        result = render_instruction_content(
            self.PROTECTED_BLOCK_TEMPLATE, [], protected_cloud=True
        )
        assert "staged for your review" in result
        assert "{%" not in result
        assert "{}" not in result

    def test_protected_block_absent_when_flag_false(self):
        from shared.runtime.core.loader import render_instruction_content

        result = render_instruction_content(
            self.PROTECTED_BLOCK_TEMPLATE, [], protected_cloud=False
        )
        assert "staged for your review" not in result
        assert "{%" not in result

    def test_protected_block_absent_by_default(self):
        from shared.runtime.core.loader import render_instruction_content

        result = render_instruction_content(self.PROTECTED_BLOCK_TEMPLATE, [])
        assert "staged for your review" not in result
        assert "{%" not in result


# =============================================================================
# Credential Structure Validation
# =============================================================================


class TestCredentialStructures:
    """Validate the credential JSONB structures for each datasource type."""

    def test_generic_env_vars(self):
        creds = {"env_vars": {"DATABASE_URL": "pg://host/db", "API_KEY": "sk-123"}}
        assert isinstance(creds["env_vars"], dict)
        assert len(creds["env_vars"]) == 2

    def test_repository_token(self):
        creds = {"auth_method": "token", "token": "ghp_xxxx"}
        assert creds["auth_method"] == "token"
        assert "ssh_key" not in creds

    def test_repository_ssh(self):
        creds = {
            "auth_method": "ssh",
            "ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\ndata",
        }
        assert creds["auth_method"] == "ssh"
        assert "token" not in creds

    def test_neo4j_credentials(self):
        creds = {"username": "neo4j", "password": "secret"}
        assert "username" in creds and "password" in creds

    def test_postgresql_credentials(self):
        creds = {"password": "secret"}
        assert "password" in creds

    def test_webdav_credentials(self):
        creds = {"username": "admin", "password": "pass"}
        assert "username" in creds and "password" in creds
