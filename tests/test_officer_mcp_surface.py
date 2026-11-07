"""The Legate's officer tools on the MCP — client contracts and rendering.

The tools exist so an assistant holding the Legate's credentials can see what
an officer is doing and give him direction. Each test pins the part that would
otherwise fail silently: the request the client actually sends, and whether the
rendering tells the truth about a note that has not been read yet.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient
from shared.orch_surface.formatters import (
    format_officer_note_result,
    format_officer_post,
    format_officer_roster,
)

PROJECT_ID = "a572e4a0-d97a-4103-91fd-92a980d6717d"
THREAD_ID = "6ce5bc4c-b773-4027-b47f-55d5308c92bb"


def _client(handler) -> AsyncCockpitClient:
    return AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )


# =========================================================================
# Client contracts
# =========================================================================


@pytest.mark.asyncio
async def test_the_note_client_posts_the_message_to_the_officer_note_route():
    seen: list[tuple[str, str, dict[str, Any]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (request.method, request.url.path, json.loads(request.content or b"{}"))
        )
        return httpx.Response(200, json={"delivered": "live"})

    client = _client(handler)
    try:
        result = await client.send_officer_note(PROJECT_ID, "Cut the theme work.")
    finally:
        await client.close()

    assert seen == [
        (
            "POST",
            f"/api/projects/{PROJECT_ID}/officer/note",
            {"message": "Cut the theme work."},
        )
    ]
    assert result["delivered"] == "live"


@pytest.mark.asyncio
async def test_the_roster_client_reads_the_fleet_route():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/officers"
        return httpx.Response(200, json={"officers": [], "total": 0})

    client = _client(handler)
    try:
        assert await client.list_officers() == {"officers": [], "total": 0}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_the_post_client_reads_the_project_officer_card():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/api/projects/{PROJECT_ID}/officer"
        return httpx.Response(200, json={"commissioned": True})

    client = _client(handler)
    try:
        assert (await client.get_project_officer(PROJECT_ID))["commissioned"] is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_a_tail_read_asks_for_the_newest_messages_by_cursor():
    """Paging from offset 0 to reach turn 113 of 113 is not a read strategy."""
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"messages": [], "total": 0})

    client = _client(handler)
    try:
        await client.get_persistent_thread_messages(
            THREAD_ID, limit=10, before="2026-08-17T09:00:00+00:00"
        )
    finally:
        await client.close()

    assert "before=2026-08-17T09%3A00%3A00%2B00%3A00" in seen[0]
    assert "offset" not in seen[0]


# =========================================================================
# Rendering
# =========================================================================


def test_the_roster_shows_commissioned_vacant_and_held_at_a_glance():
    text = format_officer_roster(
        {
            "officers": [
                {
                    "project_name": "Better Resavio",
                    "project_id": PROJECT_ID,
                    "thread_id": THREAD_ID,
                    "commissioned": True,
                    "held": None,
                    "next_wake_at": "2026-08-17T09:30:00+00:00",
                    "pending_events": 3,
                    "in_flight_jobs": 2,
                    "model": "gpt-5.6-sol",
                    "auto_pull": False,
                    "last_activity_at": "2026-08-17T06:11:33+00:00",
                },
                {
                    "project_name": "Acme Pilot",
                    "project_id": "p2",
                    "thread_id": None,
                    "commissioned": False,
                    "held": None,
                },
                {
                    "project_name": "SRW Self-Dev",
                    "project_id": "p3",
                    "thread_id": "t3",
                    "commissioned": True,
                    "held": {"kind": "conference"},
                },
            ],
            "total": 3,
        }
    )

    assert "Better Resavio" in text
    assert "vacant" in text.lower()
    assert "HELD" in text
    assert "gpt-5.6-sol" in text
    assert "2 job" in text


def test_the_roster_says_so_when_no_project_has_an_officer():
    text = format_officer_roster({"officers": [], "total": 0})
    assert "no officer" in text.lower()


def test_the_roster_distinguishes_configured_auto_pull_from_released_dispatch():
    text = format_officer_roster(
        {
            "officers": [
                {
                    "project_name": "Dark century",
                    "project_id": "p1",
                    "thread_id": "t1",
                    "commissioned": True,
                    "auto_pull": True,
                    "auto_pull_enable_available": False,
                }
            ],
            "total": 1,
        }
    )
    assert "auto-pull configured — deployment release fenced" in text


def test_the_post_renders_the_kit_with_its_floor_warning():
    text = format_officer_post(
        {
            "commissioned": True,
            "held": None,
            "officer": {
                "thread_id": THREAD_ID,
                "status": "active",
                "title": "Centurion — Better Resavio",
                "hold": None,
                "model": "gpt-5.6-sol",
            },
            "kit": {
                "build": {
                    "count": 1,
                    "in_flight": 0,
                    "ready_depth": 0,
                    "below_floor": True,
                    "spend_ceiling_daily": 7.5,
                },
                "test": {"count": 1, "in_flight": 1},
            },
            "next_wake_at": "2026-08-17T09:30:00+00:00",
            "pending_events": 3,
            "conference": None,
            "backlog": {
                "auto_pull": True,
                "auto_pull_control": {"enable_available": False},
                "worker_spend_ceiling_daily": 42.25,
            },
        },
        project_name="Better Resavio",
    )

    assert "build" in text and "BELOW FLOOR" in text
    assert "2026-08-17T09:30" in text
    assert "Pending events: 3" in text
    assert "deployment release fenced" in text
    assert "Worker spend ceiling: $42.25/day" in text
    assert "$7.5/day ceiling" in text


def test_the_post_renders_a_vacant_post_without_pretending_otherwise():
    text = format_officer_post({"commissioned": False, "officer": {}}, project_name="X")
    assert "vacant" in text.lower()


def test_the_post_surfaces_runtime_authorization_without_private_detail():
    text = format_officer_post(
        {
            "commissioned": True,
            "officer": {"thread_id": THREAD_ID, "status": "active"},
            "runtime_authorization": {
                "status": "unavailable",
                "failure_class": "refresh_expired",
                "credential_generation": 99,
            },
        },
        project_name="X",
    )

    assert "Runtime authorization: UNAVAILABLE" in text
    assert "suppressed" in text
    assert "refresh_expired" not in text
    assert "99" not in text


def test_the_post_surfaces_safe_runtime_lifecycle_observations():
    text = format_officer_post(
        {
            "commissioned": True,
            "officer": {"thread_id": THREAD_ID, "status": "active"},
            "runtime_lifecycle": {
                "observed_build_sha": "old-build",
                "expected_build_sha": "new-build",
                "drift_state": "drifted",
                "recycle_phase": "failed_retryable",
                "last_failure": "replacement_not_ready",
                "generation": "must-not-exist",
                "old_pod_uid": "must-not-exist",
            },
        },
        project_name="X",
    )

    assert "Runtime lifecycle: drifted" in text
    assert "old-build" in text and "new-build" in text
    assert "replacement_not_ready" in text
    assert "must-not-exist" not in text


def test_a_note_that_only_reached_the_queue_never_reads_as_delivered():
    text = format_officer_note_result(
        {
            "delivered": "queued",
            "thread_id": THREAD_ID,
            "next_wake_at": "2026-08-17T09:30:00+00:00",
        }
    )
    assert "queued" in text.lower()
    assert "2026-08-17T09:30" in text
    assert "read" in text.lower()


def test_a_held_note_names_the_hold_that_is_blocking_it():
    text = format_officer_note_result(
        {"delivered": "held", "thread_id": THREAD_ID, "held": {"kind": "conference"}}
    )
    assert "conference" in text.lower()


def test_a_live_note_says_it_reached_him():
    text = format_officer_note_result({"delivered": "live", "thread_id": THREAD_ID})
    assert "input queue" in text.lower()


def test_a_tail_render_does_not_advertise_offset_paging():
    """The tail window is the END of the log; 'use offset=1' would walk backwards."""
    from shared.orch_surface.formatters import format_persistent_thread_messages

    text = format_persistent_thread_messages(
        {
            "thread_id": THREAD_ID,
            "total": 590,
            "messages": [
                {
                    "turn_number": 113,
                    "role": "ai",
                    "content": "filed a sleep",
                    "created_at": "2026-08-17T06:11:33+00:00",
                }
            ],
        },
        tail=True,
    )

    assert "offset=" not in text
    assert "newest" in text.lower()
    assert "589 earlier" in text


def test_the_post_shows_what_he_has_spent_against_his_ceiling():
    """His daily token ceiling defers wakes when hit — the number belongs on the card."""
    text = format_officer_post(
        {
            "commissioned": True,
            "officer": {"thread_id": THREAD_ID, "status": "active"},
            "spend_today": {"tokens": 12345, "ceiling": 100000},
        },
        project_name="Better Resavio",
    )
    assert "12,345" in text
    assert "100,000" in text


@pytest.mark.asyncio
async def test_the_lifecycle_clients_post_to_the_officer_routes():
    seen: list[tuple[str, str, dict[str, Any]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (request.method, request.url.path, json.loads(request.content or b"{}"))
        )
        return httpx.Response(200, json={"status": "ok"})

    client = _client(handler)
    try:
        await client.commission_officer(PROJECT_ID, {"brain": {"model": "m"}})
        await client.decommission_officer(PROJECT_ID, force=True, reason="done")
        await client.hold_officer(PROJECT_ID, "window")
        await client.release_officer(PROJECT_ID)
    finally:
        await client.close()

    base = f"/api/projects/{PROJECT_ID}/officer"
    assert seen == [
        ("POST", f"{base}/commission", {"brain": {"model": "m"}}),
        ("POST", f"{base}/decommission", {"force": True, "reason": "done"}),
        ("POST", f"{base}/hold", {"note": "window"}),
        ("POST", f"{base}/release", {}),
    ]
