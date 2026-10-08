"""Golden pins: what an agent is told about its connectors, in prose.

Two surfaces describe a connector to the model:

* the **README.md workspace facts** the agent writes at every worker start,
  session attach and live attach/detach (``render_workspace_facts`` in
  ``agent.core.datasource_setup``). The input is the payload the orchestrator
  sent, so each README case runs the real ``build_datasources_payload`` on the
  canonical rows first, then adds what the agent itself annotates (MCP
  discovery status, repository clone metadata). Only the ``## Connectors``
  section is pinned; materials and layout are not connector behaviour.
* the **project KB note** the orchestrator projects per linked connector
  (``knowledge_projection.sync_datasource_knowledge``): note id, title, tags,
  content and retrieval phrases, captured from the pgvector write with Neo4j
  absent.

Regenerate: ``UPDATE_CONNECTOR_GOLDENS=1 python -m pytest
tests/test_connector_goldens_facts.py`` (see ``tests/_connector_goldens.py``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from agent.core.datasource_setup import render_workspace_facts
from orchestrator.services import agent_datasource_payload as payload_module
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services import knowledge_projection
from orchestrator.services.deployment_gates import (
    mcp_datasources_enabled,
    mcp_stdio_enabled,
)
from tests._connector_goldens import KINDS, PROJECT_ID, Golden, all_rows, resolved_row

_DECLARED_READ_ONLY_DROPPED = (
    "the payload never forwards the publisher's declared read_only flag, so "
    "the README's 'declared read-only' advisory cannot render"
)


@dataclass(frozen=True)
class ReadmeCase:
    rows: list[dict[str, Any]]
    #: Applied to the payload entries by type, as the agent annotates them.
    annotate: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: ``workspace_manager.source_repo_meta`` as the clone records it.
    repo_meta: dict[str, dict[str, Any]] | None = None
    #: Feed these entries to the renderer directly, bypassing the payload.
    raw_entries: list[dict[str, Any]] | None = None
    pinned_defect: str | None = None


def _mcp_tools(count: int) -> list[str]:
    return [f"docs_mcp__tool_{index:02d}" for index in range(count)]


README_CASES: dict[str, ReadmeCase] = {
    "no_connectors": ReadmeCase([]),
    "all_kinds_read_write": ReadmeCase(all_rows()),
    "all_kinds_read_only": ReadmeCase(all_rows(project_read_only=True)),
    "mcp_connected_with_tools": ReadmeCase(
        [resolved_row("mcp_remote")],
        annotate={"mcp": {"_mcp_status": "connected", "_mcp_tools": _mcp_tools(3)}},
    ),
    "mcp_connected_no_tools": ReadmeCase(
        [resolved_row("mcp_remote")],
        annotate={"mcp": {"_mcp_status": "connected", "_mcp_tools": []}},
    ),
    "mcp_connected_over_forty_tools": ReadmeCase(
        [resolved_row("mcp_remote")],
        annotate={"mcp": {"_mcp_status": "connected", "_mcp_tools": _mcp_tools(43)}},
    ),
    "mcp_unavailable": ReadmeCase(
        [resolved_row("mcp_stdio")],
        annotate={
            "mcp": {"_mcp_status": "unavailable: ConnectError", "_mcp_tools": []}
        },
    ),
    "repository_with_clone_meta": ReadmeCase(
        [resolved_row("repository_token"), resolved_row("repository_ssh")],
        repo_meta={
            "widgets": {"default_branch": "trunk", "read_only": True},
            "gadgets": {"read_only": False},
        },
    ),
    "repository_name_collision": ReadmeCase(
        [
            resolved_row("repository_token"),
            resolved_row(
                "repository_token",
                id="d5000b06-0000-0000-0000-000000000b06",
                name="Widgets fork",
                connection_url="https://github.com/fork/widgets.git",
                default_branch=None,
            ),
        ]
    ),
    "kb_native_is_not_listed": ReadmeCase([resolved_row("kb_native")]),
    "kb_without_root_path": ReadmeCase([resolved_row("kb", config={"root_path": ""})]),
    "email_whole_mailbox_from_address_fallback": ReadmeCase(
        [
            resolved_row(
                "email",
                credentials={"password": "mail-secret"},
                config={"access": "read", "from_address": "help@example.test"},
            )
        ]
    ),
    "generic_without_env_or_hint": ReadmeCase(
        [resolved_row("generic", credentials={}, cli_hint=None)]
    ),
    "credential_files_only": ReadmeCase(
        [
            resolved_row("kubeconfig"),
            resolved_row("ssh_key"),
            resolved_row("generic_file"),
            resolved_row("generic_file", credentials={"files": []}),
        ],
    ),
    "declared_read_only_via_payload": ReadmeCase(
        [
            resolved_row(kind, read_only=True, is_global=True)
            for kind in ("generic", "postgresql", "webdav", "email")
        ],
        pinned_defect=_DECLARED_READ_ONLY_DROPPED,
    ),
    # Reachable only if an entry carries ``read_only``; pins the renderer's
    # own branch for the D1b materializer move.
    "declared_read_only_renderer": ReadmeCase(
        [],
        raw_entries=[
            {
                "type": "repository",
                "name": "Widgets",
                "connection_url": "https://github.com/acme/widgets.git",
                "read_only": True,
            },
            {"type": "postgresql", "name": "Orders DB", "read_only": True},
            {"type": "webdav", "name": "Team files", "read_only": True},
            {
                "type": "credentials",
                "name": "Vendor login",
                "read_only": True,
                "credentials": {"env_vars": {"VENDOR_USER": "alice"}},
            },
            {"type": "email", "name": "Support inbox", "read_only": True},
            {"type": "future_type", "name": "Unknown", "read_only": True},
        ],
    ),
}


@dataclass(frozen=True)
class NoteCase:
    row: dict[str, Any]
    pinned_defect: str | None = None


_UNTYPED_NOTE = (
    "falls through to the bare connector note and the database retrieval phrases"
)

_READ_WRITE_NOTE_DEFECTS = {
    **dict.fromkeys(
        ("generic_file", "kubeconfig", "ssh_key", "email", "mcp_remote", "mcp_stdio"),
        _UNTYPED_NOTE,
    ),
    "kb_native": (
        "a project's own KB is described as read-only, though the agent binds "
        "it as the writable native KB (the README omits it for that reason)"
    ),
}

NOTE_CASES: dict[str, NoteCase] = {}
for _kind in KINDS:
    NOTE_CASES[f"{_kind}/read_write"] = NoteCase(
        resolved_row(_kind), pinned_defect=_READ_WRITE_NOTE_DEFECTS.get(_kind)
    )
for _kind in ("postgresql", "neo4j", "mongodb", "webdav"):
    NOTE_CASES[f"{_kind}/read_only"] = NoteCase(
        resolved_row(_kind, project_read_only=True)
    )
NOTE_CASES.update(
    {
        "generic/credentials_stored_as_json_string": NoteCase(
            resolved_row(
                "generic",
                credentials=json.dumps({"env_vars": {"BILLING_TOKEN": "x"}}),
            )
        ),
        "generic/no_url_no_hint_no_env": NoteCase(
            resolved_row(
                "generic",
                connection_url=None,
                cli_hint=None,
                credentials={},
                description=None,
            )
        ),
        # The note's clone path slugs the connector name; the real clone uses
        # the upstream repository name (resolve_repo_clone_names).
        "repository_token/clone_path_from_name": NoteCase(
            resolved_row(
                "repository_token", name="Widgets Service", default_branch=None
            ),
            pinned_defect=(
                "the note's clone path slugs the connector name, while the "
                "clone lands under the upstream repository name"
            ),
        ),
        "kb/no_root_path": NoteCase(resolved_row("kb", config={})),
    }
)


@pytest.fixture(scope="module")
def golden():
    golden = Golden(
        "facts",
        [f"readme/{case}" for case in README_CASES]
        + [f"kb_note/{case}" for case in NOTE_CASES],
    )
    yield golden
    golden.flush()


@pytest.fixture(autouse=True)
def _gates_on(monkeypatch):
    monkeypatch.setenv("MCP_DATASOURCES_ENABLED", "true")
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")


def _payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return (
        payload_module.build_datasources_payload(
            [dict(row) for row in rows],
            dependencies=payload_module.DatasourcePayloadDependencies(
                logger=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
                mcp_datasources_enabled=mcp_datasources_enabled,
                mcp_stdio_enabled=mcp_stdio_enabled,
                connector_drivers=builtin_connector_drivers(),
                workspace_ssh_known_hosts=lambda: "",
            ),
        )
        or []
    )


def _connector_section(readme_block: str) -> list[str]:
    lines = readme_block.splitlines()
    start = lines.index("## Connectors")
    end = lines.index("## Materials")
    return lines[start:end]


@pytest.mark.parametrize("case_id", list(README_CASES))
def test_readme_connector_lines_match_golden(case_id, golden):
    case = README_CASES[case_id]
    entries = (
        [dict(entry) for entry in case.raw_entries]
        if case.raw_entries is not None
        else _payload(case.rows)
    )
    for entry in entries:
        entry.update(case.annotate.get(entry.get("type"), {}))
    workspace = SimpleNamespace(
        source_repo_meta=case.repo_meta,
        list_files=lambda _directory: [],
        exists=lambda _path: False,
    )

    block = render_workspace_facts(entries, workspace)

    result: dict[str, Any] = {"lines": _connector_section(block)}
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    golden.check(f"readme/{case_id}", result)


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("case_id", list(NOTE_CASES))
async def test_kb_projection_note_matches_golden(case_id, golden):
    case = NOTE_CASES[case_id]
    conn = SimpleNamespace(execute=AsyncMock())
    dependencies = knowledge_projection.KnowledgeProjectionDependencies(
        store=SimpleNamespace(acquire=lambda: _Acquire(conn)),
        logger=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
        graph=SimpleNamespace(get=lambda: None),
    )

    await knowledge_projection.sync_datasource_knowledge(
        PROJECT_ID, dict(case.row), strict=True, dependencies=dependencies
    )

    (_sql, note_id, project_id, title, tags, content, retrieval) = (
        conn.execute.await_args.args
    )
    result: dict[str, Any] = {
        "note_id": note_id,
        "project_id": project_id,
        "title": title,
        "tags": tags,
        "content": content.splitlines(),
        "retrieval_messages": retrieval,
    }
    if case.pinned_defect:
        result["pinned_defect"] = case.pinned_defect
    golden.check(f"kb_note/{case_id}", result)


def test_golden_covers_every_case(golden):
    golden.assert_covers_cases()
