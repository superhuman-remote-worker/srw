"""Drift enumeration across connectors, projects and grants."""

from __future__ import annotations

import pytest

from orchestrator.services.config_drift import (
    DriftItem,
    blocking_denials,
    collect_config_drift,
)
from orchestrator.services.datasource_policy import ItemVerdict


DS_GONE = "d7555d5d-ce46-49e2-b1fa-8235d720badc"
DS_OK = "2991589e-249d-4cca-98ce-780db69b2520"
DS_REVOKED = "33333333-3333-4333-8333-333333333333"
PROJECT_GONE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


class _ProjectVerdict:
    def __init__(self, project_id, denied, reason):
        self.project_id = project_id
        self.denied = denied
        self.reason = reason


@pytest.mark.asyncio
async def test_no_drift_returns_empty():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_OK, False, None)],
        grant_violations=[],
    )
    assert items == []


@pytest.mark.asyncio
async def test_undenied_connector_is_not_drift_even_with_an_ack_reason():
    """Pins the `denied` half of the guard independently of the reason half:
    a reason string alone must never produce an item."""
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_OK, False, "deleted")],
        grant_violations=[],
        tombstones={DS_OK: "Should Not Appear"},
    )
    assert items == []


@pytest.mark.asyncio
async def test_undenied_project_is_not_drift_even_with_an_ack_reason():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[_ProjectVerdict(PROJECT_GONE, False, "deleted")],
        datasource_ids=[],
        grant_violations=[],
    )
    assert items == []


@pytest.mark.asyncio
async def test_deleted_connector_named_from_tombstone():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_GONE, True, "deleted")],
        grant_violations=[],
        tombstones={DS_GONE: "WidgetShop"},
    )
    assert items == [
        DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "WidgetShop")
    ]


@pytest.mark.asyncio
async def test_deleted_connector_without_tombstone_falls_back_to_uuid():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_GONE, True, "deleted")],
        grant_violations=[],
    )
    assert items[0].label == DS_GONE


@pytest.mark.asyncio
async def test_revoked_connector_is_not_named():
    """Naming a revoked connector would confirm it still exists and reveal its
    current name — a genuine enumeration oracle. Deleted rows carry no such
    risk, which is why only they are named."""
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_REVOKED, True, "revoked")],
        grant_violations=[],
        tombstones={DS_REVOKED: "Should Not Appear"},
    )
    assert items[0].label == "a connector you no longer have access to"
    assert "Should Not Appear" not in items[0].label


@pytest.mark.asyncio
async def test_out_of_scope_connector_is_reported_generically():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_REVOKED, True, "out_of_scope")],
        grant_violations=[],
    )
    assert items[0].reason == "out_of_scope"
    assert items[0].label == "a connector you no longer have access to"


@pytest.mark.asyncio
async def test_workspace_tier_verdict_is_not_drift():
    """A lite-tier repository conflict is a config incompatibility, not
    something an acknowledgment can resolve. It must keep raising 400."""
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_OK, True, "workspace_tier")],
        grant_violations=[],
    )
    assert items == []


@pytest.mark.asyncio
async def test_corrupt_revision_verdict_is_not_drift():
    """Corruption is not drift: no acknowledgment can make a bad
    policy_revision safe, so it must keep failing closed rather than
    appearing as a dismissible item."""
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[ItemVerdict(DS_OK, True, "corrupt_revision")],
        grant_violations=[],
    )
    assert items == []


@pytest.mark.asyncio
async def test_deleted_project_reported():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[_ProjectVerdict(PROJECT_GONE, True, "deleted")],
        datasource_ids=[],
        grant_violations=[],
    )
    assert items == [
        DriftItem(
            f"project:{PROJECT_GONE}",
            "project",
            "deleted",
            "a project that no longer exists",
        )
    ]


@pytest.mark.asyncio
async def test_grant_violation_parsed_into_item():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[],
        grant_violations=["shell_tools: tools.shell requires the shell_tools grant"],
    )
    assert items == [
        DriftItem(
            "grant:shell_tools",
            "grant",
            "revoked",
            "tools.shell requires the shell_tools grant",
        )
    ]


@pytest.mark.asyncio
async def test_malformed_grant_violation_still_yields_an_item():
    items = await collect_config_drift(
        None,
        {},
        owner={"id": "u1"},
        project_ids=[],
        datasource_ids=[],
        grant_violations=["no colon here"],
    )
    assert items[0].id == "grant:no colon here"
    assert items[0].label == "no colon here"


def test_blocking_denials_flags_workspace_tier_and_corrupt_revision():
    """B: no acknowledgment can make either safe, so both must refuse at
    resume rather than silently vanish the way collect_config_drift treats
    them (see test_workspace_tier_verdict_is_not_drift /
    test_corrupt_revision_verdict_is_not_drift above)."""
    ids = blocking_denials(
        [
            ItemVerdict(DS_OK, True, "workspace_tier"),
            ItemVerdict(DS_GONE, True, "corrupt_revision"),
        ],
        [],
    )
    assert ids == [f"connector:{DS_OK}", f"connector:{DS_GONE}"]


def test_blocking_denials_ignores_acknowledgeable_and_undenied_verdicts():
    ids = blocking_denials(
        [
            ItemVerdict(DS_GONE, True, "deleted"),
            ItemVerdict(DS_REVOKED, True, "revoked"),
            ItemVerdict(DS_OK, True, "out_of_scope"),
            ItemVerdict(DS_OK, False, None),
        ],
        [
            _ProjectVerdict(PROJECT_GONE, True, "deleted"),
            _ProjectVerdict(PROJECT_GONE, True, "revoked"),
            _ProjectVerdict(PROJECT_GONE, False, None),
        ],
    )
    assert ids == []


def test_blocking_denials_also_flags_project_verdicts():
    """The project half of the same generic rule, in case a project verdict
    ever carries a non-acknowledgeable reason — the function must not be
    silently connector-only."""
    ids = blocking_denials(
        [], [_ProjectVerdict(PROJECT_GONE, True, "corrupt_revision")]
    )
    assert ids == [f"project:{PROJECT_GONE}"]
