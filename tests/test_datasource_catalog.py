"""Drift checks for the shipped connector-type inventory and product guide."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from orchestrator.schemas.datasources import DatasourceCreate
from orchestrator.security.credential_files import (
    CREDENTIAL_FILE_TYPES as ORCHESTRATOR_CREDENTIAL_FILE_TYPES,
)
from shared.runtime.core.datasource_catalog import (
    DATASOURCE_TYPE_CATALOG,
    DATASOURCE_TYPE_IDS,
    DATASOURCE_TYPES,
)
from agent.core.datasource_setup import CREDENTIAL_FILE_TYPES, DATASOURCE_TOOL_MAP

_ROOT = Path(__file__).resolve().parents[1]
_GUIDE = _ROOT / "config" / "skills" / "app-guide"


def test_datasource_catalog_is_unique_and_complete():
    assert DATASOURCE_TYPE_IDS
    assert len(DATASOURCE_TYPE_IDS) == len(DATASOURCE_TYPES)
    assert DATASOURCE_TYPE_IDS == tuple(
        definition.type_id for definition in DATASOURCE_TYPE_CATALOG
    )
    assert all(definition.title for definition in DATASOURCE_TYPE_CATALOG)
    assert all(definition.guide_topic for definition in DATASOURCE_TYPE_CATALOG)


def test_datasource_catalog_matches_agent_consumers():
    credential_types = {
        definition.type_id
        for definition in DATASOURCE_TYPE_CATALOG
        if definition.runtime_kind == "credential_file"
    }
    # "repository" keeps its own runtime_kind — it clones onto the workspace
    # instead of opening a connection — but it is tool-backed too now (the
    # repo_* category), so it belongs in this side of the drift check.
    tool_types = {
        definition.type_id
        for definition in DATASOURCE_TYPE_CATALOG
        if definition.runtime_kind
        in {"managed_tools", "email_tools", "mcp_tools", "repository"}
    }

    assert credential_types == CREDENTIAL_FILE_TYPES
    assert credential_types == ORCHESTRATOR_CREDENTIAL_FILE_TYPES
    assert tool_types == set(DATASOURCE_TOOL_MAP)


def test_datasource_catalog_matches_the_mcp_server():
    import typing

    from mcp_server.server import DatasourceType

    assert typing.get_args(DatasourceType) == DATASOURCE_TYPE_IDS


def test_orchestrator_validation_consumes_the_catalog():
    # Create accepts exactly the types the installed drivers serve, and the
    # built-in drivers serve the catalogue's types in catalogue order.
    from orchestrator.services.connector_drivers import builtin_connector_drivers

    source = (_ROOT / "src" / "orchestrator" / "services" / "datasources.py").read_text(
        encoding="utf-8"
    )
    create_route = source.split("async def create_datasource(", 1)[1].split(
        "\n\nasync def update_datasource(", 1
    )[0]

    assert "dependencies.connector_drivers.for_type(body.type)" in create_route
    assert builtin_connector_drivers().type_ids() == DATASOURCE_TYPE_IDS
    assert (
        DatasourceCreate.model_json_schema()["properties"]["type"]["description"]
        == f"Connector type: {', '.join(DATASOURCE_TYPE_IDS)}"
    )


def test_datasource_catalog_matches_cockpit_consumers():
    model = (
        _ROOT / "cockpit" / "src" / "app" / "core" / "models" / "api.model.ts"
    ).read_text(encoding="utf-8")
    component = (
        _ROOT
        / "cockpit"
        / "src"
        / "app"
        / "views"
        / "datasources"
        / "datasource-list.component.ts"
    ).read_text(encoding="utf-8")

    type_union = model.split("export type DatasourceType =", 1)[1].split(";", 1)[0]
    model_types = set(re.findall(r"'([a-z][a-z0-9_]*)'", type_union))

    selector_value = component.index('[value]="formData.type"')
    selector_start = component.rfind("<app-select", 0, selector_value)
    selector_end = component.index("</app-select>", selector_start)
    form_types = set(
        re.findall(
            r'<option value="([a-z][a-z0-9_]*)">',
            component[selector_start:selector_end],
        )
    )

    filters_start = component.index("readonly typeFilters = [")
    filters_end = component.index("];", filters_start)
    filter_types = set(
        re.findall(
            r"value: '([a-z][a-z0-9_]*)'",
            component[filters_start:filters_end],
        )
    )
    filter_types.discard("all")

    assert model_types == DATASOURCE_TYPES
    assert form_types == DATASOURCE_TYPES
    assert filter_types == DATASOURCE_TYPES


def test_every_datasource_type_has_routable_guide_coverage():
    skill = (_GUIDE / "SKILL.md").read_text(encoding="utf-8")

    for definition in DATASOURCE_TYPE_CATALOG:
        reference = _GUIDE / "references" / f"{definition.guide_topic}.md"
        assert reference.is_file(), definition.type_id
        assert f"`{definition.guide_topic}`" in skill, definition.type_id
        assert f"`references/{definition.guide_topic}.md`" in skill, definition.type_id
        assert (
            definition.title.lower() in reference.read_text(encoding="utf-8").lower()
        ), definition.type_id


def test_focused_connector_guides_keep_metadata_and_safety_boundaries():
    expected = {
        "datasources-email": {
            "guide_id": "datasources.email.connect",
            "capability_ids": {"datasources.email", "datasources.email.send"},
            "journey_ids": {"datasources.email.create"},
        },
        "datasources-okf": {
            "guide_id": "datasources.okf.connect",
            "capability_ids": {"datasources.okf"},
            "journey_ids": {"datasources.okf.create"},
        },
    }

    bodies: dict[str, str] = {}
    for topic, wanted in expected.items():
        text = (_GUIDE / "references" / f"{topic}.md").read_text(encoding="utf-8")
        metadata_text, body = text.removeprefix("---\n").split("\n---\n", 1)
        metadata = yaml.safe_load(metadata_text)
        assert metadata["guide_id"] == wanted["guide_id"]
        assert metadata["content_type"] == "how_to"
        assert set(metadata["capability_ids"]) == wanted["capability_ids"]
        assert set(metadata["journey_ids"]) == wanted["journey_ids"]
        bodies[topic] = " ".join(body.lower().split())

    email = bodies["datasources-email"]
    assert "leaving this empty shares the whole mailbox" in email
    assert "cannot be published" in email
    assert "email_autonomous_send" in email
    assert "does not yet provide the planned human-approval queue" in email

    okf = bodies["datasources-okf"]
    assert "read-only to agents" in okf
    assert "test connection" in okf
    assert "full rebuild" in okf
    assert "periodic orchestrator sweep" in okf
