"""Golden pins: the datasources payload the orchestrator sends to the agent.

``build_datasources_payload`` (with ``apply_cloud_storage_override`` before it,
as both dispatch paths call them) is the only place a resolved connector row
becomes something the agent receives. The agent payload is an untyped list and
agent images roll independently of the orchestrator, so D1 must keep it
byte-identical (``connector_drivers.md``, "Wire compatibility"). Each case
compares the compact JSON serialisation, key order included.

Input rows are shaped like ``resolve_datasources_for_job`` returns them (see
``tests/_connector_goldens.py``). The MCP deployment gates are the real
``deployment_gates`` functions, steered by the environment as in production.

Regenerate: ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest
tests/test_connector_goldens_payload.py`` (see ``tests/_connector_goldens.py``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.services import agent_datasource_payload as payload_module
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.deployment_gates import (
    mcp_datasources_enabled,
    mcp_stdio_enabled,
)
from tests._connector_goldens import (
    KINDS,
    Golden,
    all_rows,
    resolved_row,
)


@dataclass(frozen=True)
class PayloadCase:
    rows: list[dict[str, Any]]
    job_context: dict[str, Any] = field(default_factory=dict)
    mcp: bool = True
    stdio: bool = True
    pinned_defect: str | None = None


_RO_CREDENTIALS_DROPPED = (
    "D1 fixes (L1 §6 #4): a read-only managed link withholds credentials, so a "
    "read-only neo4j/webdav connector reaches the agent without its username "
    "and password and cannot log in"
)

CASES: dict[str, PayloadCase] = {}
for _kind in KINDS:
    CASES[f"{_kind}/read_write"] = PayloadCase([resolved_row(_kind)])
    CASES[f"{_kind}/read_only"] = PayloadCase(
        [resolved_row(_kind, project_read_only=True)],
        pinned_defect=(
            _RO_CREDENTIALS_DROPPED if _kind in ("neo4j", "webdav") else None
        ),
    )
CASES.update(
    {
        # No project link: the LEFT JOIN yields NULL and the entry carries
        # ``project_read_only: null`` rather than false.
        "postgresql/unlinked": PayloadCase(
            [resolved_row("postgresql", project_read_only=None)]
        ),
        # The publisher's declared ``read_only`` flag is not forwarded; only
        # the per-link ``project_read_only`` is.
        "postgresql/declared_read_only": PayloadCase(
            [resolved_row("postgresql", read_only=True, is_global=True)]
        ),
        "generic/credentials_stored_as_json_string": PayloadCase(
            [
                resolved_row(
                    "generic",
                    credentials=json.dumps({"env_vars": {"BILLING_TOKEN": "x"}}),
                )
            ]
        ),
        "generic/credentials_unparseable_string": PayloadCase(
            [resolved_row("generic", credentials="{not json")]
        ),
        "generic/no_cli_hint_or_url": PayloadCase(
            [resolved_row("generic", cli_hint=None, connection_url=None)]
        ),
        "webdav/cloud_storage_override_read_only": PayloadCase(
            [resolved_row("webdav")],
            job_context={"cloud_storage_read_only": True},
            pinned_defect=_RO_CREDENTIALS_DROPPED,
        ),
        "webdav/cloud_storage_override_read_write": PayloadCase(
            [resolved_row("webdav", project_read_only=True)],
            job_context={"cloud_storage_read_only": False},
        ),
        # The override touches webdav rows only.
        "postgresql/cloud_storage_override_ignored": PayloadCase(
            [resolved_row("postgresql")],
            job_context={"cloud_storage_read_only": True},
        ),
        "repository_token/require_default_branch": PayloadCase(
            [resolved_row("repository_token", require_default_branch=True)]
        ),
        "repository_token/no_id_no_config": PayloadCase(
            [resolved_row("repository_token", id=None, config=None)]
        ),
        "email/read_only_link_floors_access": PayloadCase(
            [
                resolved_row(
                    "email",
                    project_read_only=True,
                    config={"access": "send", "folders": ["INBOX"]},
                )
            ]
        ),
        "email/unattended_send_without_owner_grant": PayloadCase(
            [
                resolved_row(
                    "email",
                    config={
                        "access": "send",
                        "folders": ["INBOX"],
                        "unattended_send": True,
                    },
                    _owner_can_autonomous_send=False,
                )
            ]
        ),
        "email/unattended_send_with_owner_grant": PayloadCase(
            [
                resolved_row(
                    "email",
                    config={
                        "access": "send",
                        "folders": ["INBOX"],
                        "unattended_send": True,
                    },
                    _owner_can_autonomous_send=True,
                )
            ]
        ),
        "email/raw_config_defaults": PayloadCase(
            [resolved_row("email", config={"folders": [" INBOX ", "", 3]})]
        ),
        "email/second_mailbox_skipped": PayloadCase(
            [
                resolved_row("email"),
                resolved_row(
                    "email",
                    id="d5000b0c-0000-0000-0000-000000000b0c",
                    name="Sales inbox",
                ),
            ]
        ),
        "kb/config_needs_normalising": PayloadCase(
            [resolved_row("kb", config={"root_path": "./notes//daily/"})]
        ),
        "mcp_remote/datasources_gate_off": PayloadCase(
            [resolved_row("mcp_remote")], mcp=False
        ),
        "mcp_stdio/datasources_gate_off": PayloadCase(
            [resolved_row("mcp_stdio")], mcp=False
        ),
        "mcp_stdio/stdio_gate_off": PayloadCase(
            [resolved_row("mcp_stdio")], stdio=False
        ),
        "mcp_remote/stdio_gate_off_keeps_remote": PayloadCase(
            [resolved_row("mcp_remote")], stdio=False
        ),
        "mixed/all_kinds_read_write": PayloadCase(all_rows()),
        "mixed/all_kinds_read_only": PayloadCase(
            all_rows(project_read_only=True), pinned_defect=_RO_CREDENTIALS_DROPPED
        ),
        "mixed/all_kinds_gates_off": PayloadCase(all_rows(), mcp=False, stdio=False),
        # Key order after the driver's own fields: cli_hint, default_branch,
        # then require_default_branch.
        "generic/cli_hint_and_default_branch": PayloadCase(
            [resolved_row("generic", default_branch="release")]
        ),
        "repository_token/every_trailing_key": PayloadCase(
            [
                resolved_row(
                    "repository_token",
                    cli_hint="clone with care",
                    require_default_branch=True,
                )
            ]
        ),
        # A stored type no driver serves is forwarded as stored.
        "unknown_type/forwarded_as_stored": PayloadCase(
            [
                resolved_row(
                    "generic",
                    type="ftp",
                    default_branch="main",
                    project_read_only=True,
                )
            ]
        ),
        "empty": PayloadCase([]),
    }
)


@pytest.fixture(scope="module")
def golden():
    golden = Golden("payload", CASES)
    yield golden
    golden.flush()


@pytest.fixture
def gates(monkeypatch):
    def apply(case: PayloadCase) -> None:
        for name, on in (
            ("MCP_DATASOURCES_ENABLED", case.mcp),
            ("MCP_STDIO_ENABLED", case.stdio),
        ):
            if on:
                monkeypatch.setenv(name, "true")
            else:
                monkeypatch.delenv(name, raising=False)

    return apply


def build_payload(case: PayloadCase) -> tuple[list[dict] | None, list[str]]:
    """Run the dispatch order: cloud override, then the payload builder."""
    logged: list[str] = []
    logger = SimpleNamespace(warning=lambda msg, *args: logged.append(msg % args))
    rows = [dict(row) for row in case.rows]
    payload_module.apply_cloud_storage_override(rows, dict(case.job_context))
    payload = payload_module.build_datasources_payload(
        rows,
        dependencies=payload_module.DatasourcePayloadDependencies(
            logger=logger,
            mcp_datasources_enabled=mcp_datasources_enabled,
            mcp_stdio_enabled=mcp_stdio_enabled,
            connector_drivers=builtin_connector_drivers(),
        ),
    )
    return payload, logged


@pytest.mark.parametrize("case_id", list(CASES))
def test_payload_matches_golden(case_id, golden, gates):
    case = CASES[case_id]
    gates(case)

    payload, warnings = build_payload(case)

    result: dict[str, Any] = {"payload": payload}
    if warnings:
        result["warnings"] = warnings
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    golden.check(case_id, result, exact_bytes=True)


def test_golden_covers_every_case(golden):
    golden.assert_covers_cases()
