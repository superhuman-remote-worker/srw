"""Tests for the shared datasource→tool-category map (live_session_settings.md P0.2).

The map lives once in ``src/core/datasource_setup.datasource_tool_categories``
and both trust boundaries delegate to it: the orchestrator's
``_build_datasource_tool_override`` and the agent's session attach path
(including the hydrated-attach fold via
``_apply_datasource_enrichment_to_resolved``). Before this, two
hand-maintained copies disagreed on read-write managed connectors
(agent: write tools; orchestrator: CLI-only).
"""

import pytest

from agent.core.datasource_setup import (
    DATASOURCE_TOOL_MAP,
    datasource_tool_categories,
)
from orchestrator.application import preparation as preparation_composition
from orchestrator.application.resources import bound
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)


def _ds(ds_type: str, read_only: bool = False, name: str = "ds") -> dict:
    return {"type": ds_type, "name": name, "project_read_only": read_only}


class TestDatasourceToolCategories:
    def test_no_datasources_strips_all_categories(self):
        assert datasource_tool_categories([]) == {
            "graph": [],
            "sql": [],
            "mongodb": [],
            "webdav": [],
            "repo": [],
            "email": [],
            "mcp": [],
        }

    def test_read_only_managed_connector_gets_read_tools(self):
        cats = datasource_tool_categories([_ds("postgresql", read_only=True)])
        assert cats["sql"] == ["sql_query", "sql_schema"]
        # Other categories stripped, not omitted — stale tools from a
        # previously attached datasource must not survive.
        assert cats["graph"] == []
        assert cats["mongodb"] == []
        assert cats["webdav"] == []

    def test_read_write_managed_connector_gets_write_tools(self):
        """RW postgresql/neo4j/mongodb → connection-backed write tools.
        Previously ``[]`` ("CLI mode"), which was dead on remote backends
        and left RW datasources with no access path at all
        (knowledge-base/knowledge/issues/datasource_cli_mode_dead_on_remote.md, direction 1)."""
        for ds_type, category, write_tool in (
            ("postgresql", "sql", "sql_execute"),
            ("neo4j", "graph", "cypher_execute"),
            ("mongodb", "mongodb", "mongo_insert"),
        ):
            cats = datasource_tool_categories([_ds(ds_type, read_only=False)])
            assert cats[category] == DATASOURCE_TOOL_MAP[ds_type]["write"], ds_type
            assert write_tool in cats[category], ds_type

    def test_read_write_webdav_gets_write_tools(self):
        cats = datasource_tool_categories([_ds("webdav", read_only=False)])
        assert "webdav_write" in cats["webdav"]
        assert "webdav_delete" in cats["webdav"]

    def test_mixed_same_type_any_read_write_wins(self):
        """Multiple datasources of one type: ALL must be read-only for read
        tools; a single RW one flips the type to write tools (the old
        agent-side copy keyed off the FIRST entry only)."""
        cats = datasource_tool_categories(
            [
                _ds("postgresql", read_only=True, name="a"),
                _ds("postgresql", read_only=False, name="b"),
            ]
        )
        assert cats["sql"] == DATASOURCE_TOOL_MAP["postgresql"]["write"]

        cats = datasource_tool_categories(
            [
                _ds("postgresql", read_only=True, name="a"),
                _ds("postgresql", read_only=True, name="b"),
            ]
        )
        assert cats["sql"] == ["sql_query", "sql_schema"]

    def test_unmapped_types_are_ignored(self):
        # "repository" is deliberately excluded here — it is now mapped (to
        # "repo"), so it belongs in the mapped-types tests, not this one.
        cats = datasource_tool_categories(
            [_ds("kb"), _ds("generic"), {"name": "typeless"}]
        )
        assert cats == {
            "graph": [],
            "sql": [],
            "mongodb": [],
            "webdav": [],
            "repo": [],
            "email": [],
            "mcp": [],
        }

    def test_mcp_attached_yields_wildcard(self):
        cats = datasource_tool_categories([_ds("mcp")])
        assert cats["mcp"] == ["*"]

    def test_mcp_absent_strips_category(self):
        assert datasource_tool_categories([])["mcp"] == []

    def test_mcp_read_only_link_still_wildcard(self):
        # No read-only mode for MCP: the server is the access boundary.
        cats = datasource_tool_categories([_ds("mcp", read_only=True)])
        assert cats["mcp"] == ["*"]

    def test_returns_copies_not_map_references(self):
        cats = datasource_tool_categories([_ds("webdav", read_only=True)])
        cats["webdav"].append("mutated")
        assert "mutated" not in DATASOURCE_TOOL_MAP["webdav"]["read"]


def test_repository_maps_to_repo_category_not_git():
    """Reusing 'git' would strip the workspace git tools when no repo is attached."""
    from agent.core.datasource_setup import datasource_tool_categories

    cats = datasource_tool_categories(
        [{"type": "repository", "name": "r", "project_read_only": False}]
    )
    assert "repo_push" in cats["repo"]
    assert "git" not in cats


def test_read_only_repository_gets_only_read_tools():
    from agent.core.datasource_setup import datasource_tool_categories

    cats = datasource_tool_categories(
        [{"type": "repository", "name": "r", "project_read_only": True}]
    )
    assert cats["repo"] == ["repo_pull", "repo_pr_status"]


class TestRepoCategorySurvivesTheRealFunnel:
    """Category maps are useless if the loader drops the category.

    ``datasource_tool_categories`` returning ``{"repo": [...]}`` proves
    nothing on its own: the override travels through ``ToolsConfig`` (a
    dataclass with a fixed field list) and out through ``get_all_tool_names``
    (a hardcoded category tuple), which is the single funnel both
    ``src/agent.py`` and ``src/api/persistent_session.py`` use to decide what
    to load. A category missing from EITHER place is silently discarded, so
    ``"repo" in tools_by_category`` in the registry is never true and the
    whole toolkit is dead code. These tests walk the real funnel.
    """

    def _config_for(self, datasources):
        import orchestrator.main
        from shared.runtime.core.loader import load_config_from_resolved

        override = agent_datasource_payload_module.build_datasource_tool_override(
            datasources,
            None,
            dependencies=preparation_composition.datasource_payload_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        resolved = {
            "agent": {
                "agent_id": "a",
                "display_name": "A",
                "tools": override["tools"],
            },
            "prompts": {},
            "instructions": {},
        }
        return load_config_from_resolved(resolved)

    def test_tools_config_has_a_repo_field(self):
        """A dataclass without the field silently drops the category."""
        from shared.runtime.core.loader import ToolsConfig

        assert "repo" in ToolsConfig.__dataclass_fields__
        assert ToolsConfig().repo == []

    def test_loader_keeps_the_repo_category(self):
        config = self._config_for([_ds("repository")])
        assert config.tools.repo == [
            "repo_checkout",
            "repo_commit",
            "repo_push",
            "repo_pull",
            "repo_open_pr",
            "repo_pr_status",
        ]

    def test_get_all_tool_names_yields_repo_tools(self):
        """The guard for the whole feature: without ``"repo"`` in the
        ``get_all_tool_names`` category tuple, the agent never asks the
        registry for a single repo tool."""
        from shared.runtime.core.loader import get_all_tool_names

        names = get_all_tool_names(self._config_for([_ds("repository")]))
        assert "repo_checkout" in names
        assert "repo_commit" in names
        assert "repo_push" in names
        assert "repo_pull" in names
        assert "repo_open_pr" in names
        assert "repo_pr_status" in names

    def test_read_only_repository_yields_only_repo_read_tools(self):
        from shared.runtime.core.loader import get_all_tool_names

        names = get_all_tool_names(
            self._config_for([_ds("repository", read_only=True)])
        )
        assert "repo_pull" in names
        assert "repo_pr_status" in names
        assert "repo_checkout" not in names
        assert "repo_commit" not in names
        assert "repo_push" not in names
        assert "repo_open_pr" not in names

    def test_no_repository_attached_yields_no_repo_tools(self):
        from shared.runtime.core.loader import get_all_tool_names

        names = get_all_tool_names(self._config_for([_ds("postgresql")]))
        assert not [n for n in names if n.startswith("repo_")]

    def test_registry_sees_the_repo_category(self):
        """End of the funnel: the name list the agent hands the registry must
        group back into a ``repo`` bucket, which is what gates the wiring
        block in ``src/tools/registry.py`` (``if "repo" in
        tools_by_category``)."""
        from shared.runtime.core.loader import get_all_tool_names
        from agent.tools.registry import TOOL_REGISTRY

        names = get_all_tool_names(self._config_for([_ds("repository")]))
        # Same grouping load_tools() performs before dispatching by category.
        categories = {TOOL_REGISTRY[n].get("category") for n in names}
        assert "repo" in categories


class TestEmailTierCategories:
    """Email is tier-keyed (config.access), not binary read/write."""

    def _email_ds(self, access=None, read_only=False, name="mail"):
        ds = _ds("email", read_only=read_only, name=name)
        if access is not None:
            ds["config"] = {"access": access}
        return ds

    def test_tiers_are_cumulative(self):
        read = datasource_tool_categories([self._email_ds("read")])["email"]
        rw = datasource_tool_categories([self._email_ds("read_write")])["email"]
        draft = datasource_tool_categories([self._email_ds("draft")])["email"]
        send = datasource_tool_categories([self._email_ds("send")])["email"]
        assert set(read) == {
            "email_list_folders",
            "email_list",
            "email_search",
            "email_read",
        }
        assert set(rw) == set(read) | {"email_move", "email_flag"}
        assert set(draft) == set(rw) | {"email_draft"}
        assert set(send) == set(draft) | {"email_send"}

    def test_missing_config_defaults_to_draft(self):
        cats = datasource_tool_categories([_ds("email")])
        assert "email_draft" in cats["email"]
        assert "email_send" not in cats["email"]

    def test_project_read_only_clamps_to_read(self):
        cats = datasource_tool_categories([self._email_ds("send", read_only=True)])
        assert "email_read" in cats["email"]
        assert "email_move" not in cats["email"]
        assert "email_send" not in cats["email"]

    def test_invalid_access_fails_closed_to_read(self):
        cats = datasource_tool_categories([self._email_ds("root")])
        assert set(cats["email"]) == {
            "email_list_folders",
            "email_list",
            "email_search",
            "email_read",
        }


class TestOrchestratorDelegation:
    """The orchestrator wrapper must produce exactly the shared map's
    categories while preserving unrelated override content."""

    @pytest.fixture(scope="class")
    def build_override(self):
        import orchestrator.main

        return bound(
            agent_datasource_payload_module.build_datasource_tool_override,
            preparation_composition.datasource_payload_dependencies,
            orchestrator.main.app.state.resources,
        )

    def test_wrapper_matches_shared_function(self, build_override):
        datasources = [
            _ds("postgresql", read_only=True),
            _ds("webdav", read_only=False),
        ]
        override = build_override(datasources, None)
        assert override["tools"] == datasource_tool_categories(datasources)

    def test_wrapper_preserves_non_datasource_categories(self, build_override):
        override = build_override(
            [_ds("neo4j", read_only=True)],
            {"llm": {"model": "m"}, "tools": {"shell": ["run_command"]}},
        )
        assert override["llm"] == {"model": "m"}
        assert override["tools"]["shell"] == ["run_command"]
        assert override["tools"]["graph"] == ["cypher_query", "get_database_schema"]


class TestApplyDatasourceEnrichmentToResolved:
    """Hydrated attaches load resolved_config["agent"] and skip the
    config_override merge — the enrichment must be folded into the blob
    (dedicated-pod parity with warm-pool, live_session_settings.md P0.2)."""

    def test_folds_categories_and_cli_types_into_agent_dict(self):
        from agent.api.persistent_app import _apply_datasource_enrichment_to_resolved

        resolved = {
            "agent": {
                "agent_id": "a",
                "tools": {"core": ["read_file"], "sql": ["stale_tool"]},
            },
            "prompts": {},
        }
        cats = {"sql": ["sql_query", "sql_schema"], "graph": []}
        _apply_datasource_enrichment_to_resolved(resolved, cats, ["postgresql"])

        agent = resolved["agent"]
        # Categories merged; unrelated categories preserved; stale replaced.
        assert agent["tools"]["sql"] == ["sql_query", "sql_schema"]
        assert agent["tools"]["graph"] == []
        assert agent["tools"]["core"] == ["read_file"]
        # _cli_datasources goes at the TOP level of the agent dict —
        # serialize_resolved_config flattens extra there, and
        # load_agent_config_from_dict folds unknown top-level keys back
        # into config.extra (which loader.py reads at prompt render).
        assert agent["_cli_datasources"] == ["postgresql"]
        assert "extra" not in agent

    def test_top_level_cli_key_reaches_config_extra_via_loader(self):
        """End-to-end through the real loader: the enrichment written by
        _apply_datasource_enrichment_to_resolved must surface as
        config.extra['_cli_datasources'] and config.tools.sql after
        load_config_from_resolved."""
        from agent.api.persistent_app import _apply_datasource_enrichment_to_resolved
        from shared.runtime.core.loader import load_config_from_resolved

        resolved = {
            "agent": {"agent_id": "a", "display_name": "A", "tools": {}},
            "prompts": {},
            "instructions": {},
        }
        _apply_datasource_enrichment_to_resolved(
            resolved, {"sql": ["sql_query", "sql_schema"]}, ["postgresql"]
        )
        config = load_config_from_resolved(resolved)
        assert config.extra["_cli_datasources"] == ["postgresql"]
        assert config.tools.sql == ["sql_query", "sql_schema"]

    def test_noop_on_missing_or_malformed_blob(self):
        from agent.api.persistent_app import _apply_datasource_enrichment_to_resolved

        # None blob: nothing to do, must not raise.
        _apply_datasource_enrichment_to_resolved(None, {"sql": []}, ["postgresql"])

        # Malformed agent key: left untouched.
        resolved = {"agent": "not-a-dict"}
        _apply_datasource_enrichment_to_resolved(resolved, {"sql": []}, ["x"])
        assert resolved == {"agent": "not-a-dict"}

    def test_no_cli_types_leaves_top_level_unset(self):
        from agent.api.persistent_app import _apply_datasource_enrichment_to_resolved

        resolved = {"agent": {"agent_id": "a", "tools": {}}}
        _apply_datasource_enrichment_to_resolved(resolved, {"sql": []}, [])
        assert "_cli_datasources" not in resolved["agent"]


class TestProcessDatasourcesConnectionRouting:
    """Read-write managed connectors get real connections now — the CLI-mode
    routing (env injection, no connection) is retired
    (knowledge-base/knowledge/issues/datasource_cli_mode_dead_on_remote.md, direction 1)."""

    @pytest.fixture
    def spies(self, monkeypatch):
        """Spy on connection creation + the retired CLI injectors."""
        import agent.core.datasource_setup as mod
        from unittest.mock import MagicMock

        created = []

        def _fake_create(ds):
            conn = MagicMock(name=f"conn-{ds['name']}")
            conn.ds_name = ds["name"]
            created.append(ds)
            return conn, None

        monkeypatch.setattr(mod, "create_datasource_connection", _fake_create)
        cli_spies = {}
        for fn in (
            "inject_postgresql_services",
            "inject_mongodb_env_vars",
            "inject_neo4j_env_vars",
        ):
            spy = MagicMock(name=fn)
            monkeypatch.setattr(mod, fn, spy)
            cli_spies[fn] = spy
        return created, cli_spies

    def test_read_write_connector_gets_connection_not_cli(self, spies):
        from agent.core.datasource_setup import process_datasources

        created, cli_spies = spies
        connections, clients, cli_types = process_datasources(
            [_ds("postgresql", read_only=False, name="rw-db")]
        )

        assert "postgresql" in connections
        assert [ds["name"] for ds in created] == ["rw-db"]
        assert cli_types == []
        for fn, spy in cli_spies.items():
            spy.assert_not_called()

    def test_read_only_connector_unchanged(self, spies):
        from agent.core.datasource_setup import process_datasources

        created, _ = spies
        connections, clients, cli_types = process_datasources(
            [_ds("neo4j", read_only=True, name="ro-graph")]
        )
        assert "neo4j" in connections
        assert cli_types == []

    def test_mixed_same_type_read_write_connection_wins_registry(self, spies):
        """The registry is TYPE-keyed last-one-wins, and the category map
        grants write tools when ANY of a type is RW — so the RW connection
        must win the slot regardless of input order (write tools must never
        bind to the read-only-linked connection)."""
        from agent.core.datasource_setup import process_datasources

        for order in (["rw", "ro"], ["ro", "rw"]):
            connections, _, _ = process_datasources(
                [
                    _ds("postgresql", read_only=(label == "ro"), name=label)
                    for label in order
                ]
            )
            assert connections["postgresql"].ds_name == "rw", order


class TestDatasourceIndexNotes:
    """The README.md connector list must describe working access paths — the
    RW CLI usage lines (PGSERVICE/cypher-shell/mongosh) advertised a dead path."""

    def _render_index(self, ds_configs):
        from agent.core.datasource_setup import inject_workspace_facts
        from unittest.mock import MagicMock

        ws = MagicMock()
        ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        inject_workspace_facts(ds_configs, ws)
        return written.get("README.md", "")

    def test_read_write_database_entry_describes_tools_not_cli(self):
        content = self._render_index(
            [_ds("postgresql", read_only=False, name="Sales DB")]
        )
        assert "**Sales DB** (postgresql, read-write) — query + write tools" in content
        for dead_hint in ("PGSERVICE", "psql", "cypher-shell", "mongosh"):
            assert dead_hint not in content

    def test_read_only_database_entry_unchanged(self):
        content = self._render_index([_ds("neo4j", read_only=True, name="KG")])
        assert "**KG** (neo4j, read-only) — query tools" in content
