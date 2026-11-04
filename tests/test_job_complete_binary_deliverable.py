"""job_complete accepts a binary deliverable by its byte size.

The seal's "exists and is not trivial" check read every deliverable as text.
A binary one (M3's ``output/shop.db``, a SQLite database) cannot be decoded,
so a correct job was refused as "could not be read" and had to lower its
confidence below 0.5 or park on a blocking message to a human (bench run
srw-claude-eff-02, 2026-11-04).
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from agent.core.workspace import WorkspaceManager
from agent.tools.core.job import _final_phase_data, create_job_tools
from tests._fs_backend import FilesystemTestBackend
from tests._tool_invoke import invoke_tool

JOB_ID = "job-binary-deliverable"


@pytest.fixture(autouse=True)
def _clean_caches():
    _final_phase_data.clear()
    yield
    _final_phase_data.clear()


def _job_complete(tmp_path):
    root = tmp_path / "ws"
    (root / "output").mkdir(parents=True)
    db = sqlite3.connect(root / "output" / "shop.db")
    db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO customers (name) VALUES ('Fir Kft')")
    db.commit()
    db.close()
    (root / "output" / "stub.bin").write_bytes(b"\xff\xfe\x00")
    (root / "output" / "report.md").write_text("# Report\n\n" + "Totals checked. " * 10)
    ws = WorkspaceManager(
        job_id=JOB_ID, base_path=root, backend=FilesystemTestBackend(root)
    )
    context = MagicMock()
    context.job_id = JOB_ID
    context.has_workspace.return_value = True
    context.workspace_manager = ws
    context.has_todo.return_value = False
    context.orchestrator_client = None
    context.subagent_runtime = None
    _, job_complete = create_job_tools(context)
    return job_complete


@pytest.mark.asyncio
async def test_a_binary_database_deliverable_passes_the_seal(tmp_path):
    job_complete = _job_complete(tmp_path)

    result = await invoke_tool(
        job_complete,
        {
            "summary": "Loaded the tables into output/shop.db.",
            "deliverables": ["output/shop.db", "output/report.md"],
            "confidence": 0.95,
        },
    )

    assert result.startswith("Phase marked as final."), result


@pytest.mark.asyncio
async def test_a_trivial_binary_deliverable_is_still_flagged(tmp_path):
    job_complete = _job_complete(tmp_path)

    result = await invoke_tool(
        job_complete,
        {
            "summary": "Wrote the stub.",
            "deliverables": ["output/stub.bin"],
            "confidence": 0.95,
        },
    )

    assert result.startswith("ERROR: Cannot accept confidence 95%"), result
    assert "Deliverable 'output/stub.bin' appears empty or trivial (3 bytes)" in result
