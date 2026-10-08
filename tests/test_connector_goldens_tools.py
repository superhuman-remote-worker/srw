"""Golden pins: tool categories for mixed connector sets, and the type inventory.

Two trust boundaries compute the datasource tool categories from one shared
map (``shared.datasource_policy.datasource_tool_categories``):

* the orchestrator, from the resolved rows, through
  ``build_datasource_tool_override`` (job dispatch, session create/resume,
  live config updates), which also drops MCP rows the deployment gates refuse;
* the agent's session attach, from the payload entries it received
  (``session_attach.py``, ``datasource_tool_categories(datasources)``).

Each case pins both, so a driver registry that changes either side shows up.
The inventory case pins every hardcoded copy of the type list that D1a step 1
re-derives from the specs.

Regenerate: ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest
tests/test_connector_goldens_tools.py`` (see ``tests/_connector_goldens.py``).
"""

from __future__ import annotations

import typing
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.services import agent_datasource_payload as payload_module
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.deployment_gates import (
    mcp_datasources_enabled,
    mcp_stdio_enabled,
)
from shared.datasource_policy import datasource_tool_categories
from tests._connector_goldens import Golden, all_rows, resolved_row


@dataclass(frozen=True)
class ToolCase:
    rows: list[dict[str, Any]]
    config_override: dict[str, Any] | None = None
    mcp: bool = True
    stdio: bool = True
    pinned_defect: str | None = None


_SECOND_EMAIL = "d5000b0c-0000-0000-0000-000000000b0c"

CASES: dict[str, ToolCase] = {
    "none_attached": ToolCase([]),
    "all_kinds_read_write": ToolCase(all_rows()),
    "all_kinds_read_only": ToolCase(all_rows(project_read_only=True)),
    "all_kinds_unlinked": ToolCase(all_rows(project_read_only=None)),
    "no_tool_categories_env_files_kb": ToolCase(
        [
            resolved_row(kind)
            for kind in (
                "generic",
                "credentials",
                "generic_file",
                "kubeconfig",
                "ssh_key",
                "kb",
                "kb_native",
            )
        ]
    ),
    "postgresql_read_only_and_read_write": ToolCase(
        [
            resolved_row("postgresql", project_read_only=True),
            resolved_row(
                "postgresql",
                id="d5000b08-0000-0000-0000-000000000b08",
                name="Reporting DB",
            ),
        ]
    ),
    "repositories_all_read_only": ToolCase(
        [
            resolved_row("repository_token", project_read_only=True),
            resolved_row("repository_ssh", project_read_only=True),
        ]
    ),
    "repositories_one_read_write": ToolCase(
        [
            resolved_row("repository_token", project_read_only=True),
            resolved_row("repository_ssh"),
        ]
    ),
    "email_read_only_link_floors_send": ToolCase(
        [
            resolved_row(
                "email",
                project_read_only=True,
                config={"access": "send", "folders": ["INBOX"]},
            )
        ]
    ),
    "email_unknown_access_fails_closed": ToolCase(
        [resolved_row("email", config={"access": "admin"})]
    ),
    # The payload forwards only the first mailbox, and the orchestrator's
    # categories come from the forwarded rows: both sides grant the first
    # mailbox's read tier, never the second one's send tier.
    "email_two_mailboxes_forwarded_tier": ToolCase(
        [
            resolved_row("email", config={"access": "read", "folders": ["INBOX"]}),
            resolved_row(
                "email",
                id=_SECOND_EMAIL,
                name="Sales inbox",
                config={"access": "send", "folders": ["INBOX"]},
            ),
        ],
    ),
    "mcp_datasources_gate_off": ToolCase(
        [resolved_row("mcp_remote"), resolved_row("postgresql")], mcp=False
    ),
    "mcp_stdio_gate_off_with_remote": ToolCase(
        [resolved_row("mcp_stdio"), resolved_row("mcp_remote")], stdio=False
    ),
    "mcp_stdio_gate_off_alone": ToolCase([resolved_row("mcp_stdio")], stdio=False),
    "mcp_read_only_link_still_wildcard": ToolCase(
        [resolved_row("mcp_remote", project_read_only=True)]
    ),
    "existing_override_is_merged": ToolCase(
        [resolved_row("neo4j"), resolved_row("webdav", project_read_only=True)],
        config_override={
            "llm": {"model": "golden-model"},
            "tools": {"sql": ["sql_query"], "web": ["web_search"]},
        },
    ),
}


@pytest.fixture(scope="module")
def golden():
    golden = Golden("tools", [*CASES, "inventory"])
    yield golden
    golden.flush()


@pytest.fixture
def gates(monkeypatch):
    def apply(case: ToolCase) -> None:
        for name, on in (
            ("MCP_DATASOURCES_ENABLED", case.mcp),
            ("MCP_STDIO_ENABLED", case.stdio),
        ):
            if on:
                monkeypatch.setenv(name, "true")
            else:
                monkeypatch.delenv(name, raising=False)

    return apply


@pytest.mark.parametrize("case_id", list(CASES))
def test_tool_categories_match_golden(case_id, golden, gates):
    case = CASES[case_id]
    gates(case)
    dependencies = payload_module.DatasourcePayloadDependencies(
        logger=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
        mcp_datasources_enabled=mcp_datasources_enabled,
        mcp_stdio_enabled=mcp_stdio_enabled,
        connector_drivers=builtin_connector_drivers(),
        workspace_ssh_known_hosts=lambda: "",
    )
    rows = [dict(row) for row in case.rows]

    override = payload_module.build_datasource_tool_override(
        rows, case.config_override, dependencies=dependencies
    )
    payload = payload_module.build_datasources_payload(rows, dependencies=dependencies)

    result: dict[str, Any] = {
        "orchestrator_override": override,
        "agent_session_categories": datasource_tool_categories(payload or []),
    }
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    golden.check(case_id, result)


def test_type_inventory_matches_golden(golden):
    """Every hardcoded copy of the type list, as it stands."""
    from agent.core import datasource_setup
    from mcp_server.server import DatasourceType
    from orchestrator.security.credential_files import CREDENTIAL_FILE_TYPES
    from shared.credential_connectors import ENV_CONNECTOR_TYPES
    from shared.datasource_policy import DATASOURCE_TOOL_MAP
    from shared.runtime.core.datasource_catalog import DATASOURCE_TYPE_CATALOG

    golden.check(
        "inventory",
        {
            "catalog": [
                {
                    "type_id": item.type_id,
                    "title": item.title,
                    "guide_topic": item.guide_topic,
                    "runtime_kind": item.runtime_kind,
                }
                for item in DATASOURCE_TYPE_CATALOG
            ],
            "tool_map": {
                type_id: {
                    "category": info["category"],
                    "shape": (
                        "dynamic"
                        if info.get("dynamic")
                        else "tiers"
                        if "tiers" in info
                        else "read_write"
                    ),
                }
                for type_id, info in DATASOURCE_TOOL_MAP.items()
            },
            "env_connector_types": sorted(ENV_CONNECTOR_TYPES),
            "credential_file_types_orchestrator": sorted(CREDENTIAL_FILE_TYPES),
            "credential_file_types_agent": sorted(
                datasource_setup.CREDENTIAL_FILE_TYPES
            ),
            "mcp_server_datasource_type": list(typing.get_args(DatasourceType)),
        },
    )


def test_golden_covers_every_case(golden):
    golden.assert_covers_cases()
