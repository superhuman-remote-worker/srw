"""E1 truthful-read envelope + F4-F10 regression pins (officer_supervision_surface).

E1 is a DECLARED behavior change to the shared job-read output: these
fixtures pin the four §4 distinctions (empty | unavailable | stale | partial)
and the seven toolset-review repairs. They are NOT byte-compatible with the
pre-E1 S1/S2 output — that gate deliberately does not apply here.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient
from shared.orch_surface.jobs import CallerCtx, get_descriptor, make_bound_handler
from shared.orch_surface.jobs.envelope import (
    Source,
    build_envelope,
    friendly_reason,
    overall_status,
)
from shared.runtime_actor import RuntimeActorContext

JOB_ID = "19707fa1-0000-4000-8000-000000000001"


def _client(handler) -> AsyncCockpitClient:
    return AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )


def _invoke(name: str, client: AsyncCockpitClient, caller: CallerCtx | None = None):
    return make_bound_handler(
        get_descriptor(name),
        client_provider=lambda: client,
        caller_provider=lambda: caller or CallerCtx(kind="mcp"),
    )


FULL_JOB = {
    "id": JOB_ID,
    "status": "pending_review",
    "description": "Ship the report",
    "config_name": "worker_base",
    "project_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "parent_job_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "user_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
    "priority": 7,
    "assigned_agent_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
    "created_at": "2026-08-14T08:00:00Z",
    "updated_at": "2026-08-14T09:00:00Z",
    "audit_count": 5,
    "freeze_data": {
        "freeze_type": "job_complete",
        "reason": "goal reached",
        "requires_review": True,
    },
    "error_message": None,
}


# ---------------------------------------------------------------------------
# Envelope mechanics
# ---------------------------------------------------------------------------


class TestEnvelope:
    def test_overall_status_distinguishes_the_four_states(self):
        fresh = Source(name="a")
        empty = Source(name="b", status="empty")
        stale = Source(name="c", status="stale")
        down = Source(name="d", status="unavailable", reason="timeout")
        assert overall_status([fresh]) == "fresh"
        assert overall_status([empty]) == "empty"
        assert overall_status([down]) == "unavailable"
        assert overall_status([fresh, down]) == "partial"
        assert overall_status([fresh, stale]) == "stale"

    def test_build_envelope_shape_matches_spec(self):
        envelope = build_envelope(
            scope={"project_id": "p1", "job_id": JOB_ID},
            sources=[
                Source(name="control_db"),
                Source(name="audit_db", status="unavailable", reason="timeout"),
            ],
            data={"x": 1},
        )
        assert set(envelope) == {"scope", "observed_at", "sources", "data"}
        assert envelope["scope"] == {"project_id": "p1", "job_id": JOB_ID}
        assert envelope["sources"][1] == {
            "name": "audit_db",
            "status": "unavailable",
            "reason": "timeout",
        }

    def test_friendly_reason_never_leaks_internal_urls(self):
        request = httpx.Request("GET", "http://srw-orchestrator:8085/api/jobs/x")
        response = httpx.Response(500, request=request, text="boom")
        error = httpx.HTTPStatusError("bad", request=request, response=response)
        reason = friendly_reason(error)
        assert "srw-orchestrator" not in reason
        assert "8085" not in reason
        assert reason == "HTTP 500: boom"

    def test_friendly_reason_transport_failures_stay_generic(self):
        error = httpx.ConnectError("[Errno -2] connecting to srw-orchestrator:8085")
        assert friendly_reason(error) == "could not connect to the orchestrator"


# ---------------------------------------------------------------------------
# F4 — get_job decision-grade detail
# ---------------------------------------------------------------------------


class TestGetJobDetail:
    @pytest.mark.asyncio
    async def test_restores_freeze_lineage_agent_priority_and_config(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=FULL_JOB)

        client = _client(handler)
        try:
            output = await _invoke("get_job", client)(job_id=JOB_ID)
        finally:
            await client.close()

        # F4: the formatter reads config_name (API shape) — never 'Config: N/A'.
        assert "Config: worker_base" in output
        assert "Config: N/A" not in output
        assert f"Project ID: {FULL_JOB['project_id']}" in output
        assert f"Parent job ID: {FULL_JOB['parent_job_id']}" in output
        assert f"Agent: {FULL_JOB['assigned_agent_id']}" in output
        assert "Priority: 7" in output
        assert "Freeze type: job_complete" in output
        assert "Freeze reason: goal reached" in output
        assert "Requires review: True" in output

    @pytest.mark.asyncio
    async def test_404_yields_the_friendly_not_found_form(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"detail": "Job 'x' not found"})

        client = _client(handler)
        try:
            output = await _invoke("get_job", client)(job_id=JOB_ID)
        finally:
            await client.close()

        # F6: the historical friendly form, not a raw httpx message.
        assert output == f"Job '{JOB_ID}' not found."


# ---------------------------------------------------------------------------
# F5/F10 — list_jobs rich rendering + limit contract
# ---------------------------------------------------------------------------


class TestListJobs:
    @pytest.mark.asyncio
    async def test_rich_items_carry_description_lineage_and_freeze(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[FULL_JOB])

        client = _client(handler)
        try:
            output = await _invoke("list_jobs", client)()
        finally:
            await client.close()

        assert f"--- {JOB_ID} (short: {JOB_ID[:8]}) ---" in output
        assert "Description: Ship the report" in output
        assert f"Project ID: {FULL_JOB['project_id']}" in output
        assert f"Parent job ID: {FULL_JOB['parent_job_id']}" in output
        assert "Freeze type: job_complete" in output

    @pytest.mark.asyncio
    async def test_empty_names_the_filter_and_unavailable_is_distinct(self):
        async def empty_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = _client(empty_handler)
        try:
            empty = await _invoke("list_jobs", client)(status="failed")
        finally:
            await client.close()
        assert empty == "No jobs found with status='failed'."

        async def down_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        client = _client(down_handler)
        try:
            down = await _invoke("list_jobs", client)()
        finally:
            await client.close()
        # E1: an unreachable source must NEVER masquerade as an empty result.
        assert "No jobs found" not in down
        assert down.startswith("Failed to list jobs:")
        assert "could not connect" in down

    @pytest.mark.asyncio
    async def test_limit_honored_to_server_cap_with_explicit_notice_beyond(self):
        observed: list[int] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            observed.append(int(request.url.params["limit"]))
            return httpx.Response(200, json=[])

        client = _client(handler)
        try:
            # F10: 300 was silently clamped to 100 pre-E1 — now honored.
            await _invoke("list_jobs", client)(limit=300)
            over = await _invoke("list_jobs", client)(limit=900)
        finally:
            await client.close()

        assert observed == [300, 500]
        assert "limit 900 exceeds the server cap; showing at most 500" in over

    @pytest.mark.asyncio
    async def test_paging_envelope_reaches_the_rendering(self):
        """The other cases in this class hand back a bare array, which the
        client normalises — so they would keep passing against a server that
        no longer speaks that shape. This one serves the real envelope.
        """

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "jobs": [FULL_JOB],
                    "total": 806,
                    "total_is_capped": False,
                    "has_more": True,
                    "limit": 1,
                    "offset": 0,
                    "as_of": "2026-08-20T12:00:00+00:00",
                    "filters": {
                        "status": [],
                        "include_archived_projects": False,
                    },
                },
            )

        client = _client(handler)
        try:
            output = await _invoke("list_jobs", client)()
        finally:
            await client.close()

        # How much of the set is on screen, and what was filtered away.
        assert "Found 1 of 806 job(s)" in output
        assert "archived-project jobs excluded" in output
        assert f"--- {JOB_ID} (short: {JOB_ID[:8]}) ---" in output

    @pytest.mark.asyncio
    async def test_capped_total_renders_as_n_plus(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "jobs": [FULL_JOB],
                    "total": 10000,
                    "total_is_capped": True,
                    "has_more": True,
                    "limit": 1,
                    "offset": 0,
                    "filters": {},
                },
            )

        client = _client(handler)
        try:
            output = await _invoke("list_jobs", client)()
        finally:
            await client.close()

        assert "Found 1 of 10000+ job(s)" in output

    @pytest.mark.asyncio
    async def test_empty_envelope_names_the_server_side_default(self):
        """An agent told only "no jobs found" concludes the job does not
        exist. It has to be told the server hid a category."""

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "jobs": [],
                    "total": 0,
                    "total_is_capped": False,
                    "has_more": False,
                    "limit": 20,
                    "offset": 0,
                    "filters": {
                        "status": ["failed"],
                        "include_archived_projects": False,
                    },
                },
            )

        client = _client(handler)
        try:
            output = await _invoke("list_jobs", client)(status="failed")
        finally:
            await client.close()

        assert output == (
            "No jobs found with status='failed' (archived-project jobs excluded)."
        )

    @pytest.mark.asyncio
    async def test_a_bare_array_from_an_older_orchestrator_still_renders(self):
        """Agent images can lag the orchestrator across a rollout. Degrade to
        the old shape rather than iterating a dict and rendering field names.
        """

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[FULL_JOB])

        client = _client(handler)
        try:
            output = await _invoke("list_jobs", client)()
        finally:
            await client.close()

        assert "Found 1 job(s)" in output
        assert f"--- {JOB_ID} (short: {JOB_ID[:8]}) ---" in output


# ---------------------------------------------------------------------------
# F6 — steer_job must surface the response body (409 reason)
# ---------------------------------------------------------------------------


class TestSteerErrorBody:
    @pytest.mark.asyncio
    async def test_409_reason_reaches_the_caller_without_urls(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                409, json={"detail": "job is draining; steer refused"}
            )

        client = _client(handler)
        try:
            output = await _invoke("steer_job", client)(
                job_id=JOB_ID, message="stop retrying X"
            )
        finally:
            await client.close()

        assert output == "Steer failed (409): job is draining; steer refused"
        assert "http" not in output.lower()


# ---------------------------------------------------------------------------
# F7 — caller-aware stuck threshold defaults
# ---------------------------------------------------------------------------


class TestStuckThresholdDefaults:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["mcp", "officer", "session"])
    async def test_every_lane_omits_to_the_same_server_default(self, kind: str):
        observed: list[int | None] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            raw = request.url.params.get("threshold_minutes")
            observed.append(int(raw) if raw is not None else None)
            return httpx.Response(
                200,
                json={
                    "jobs": [],
                    "threshold_minutes": int(raw) if raw is not None else 47,
                    "threshold_source": (
                        "request_override" if raw is not None else "deployment_default"
                    ),
                },
            )

        client = _client(handler)
        caller = CallerCtx(
            kind=kind,  # type: ignore[arg-type]
            project_ids=("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",)
            if kind == "officer"
            else (),
            runtime_actor=RuntimeActorContext(
                caller_kind="officer",
                project_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                thread_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                officer_incarnation=1,
                access_credential="test-access-credential",
                access_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            )
            if kind == "officer"
            else None,
        )
        try:
            await _invoke("get_stuck_jobs", client, caller)()
            await _invoke("get_stuck_jobs", client, caller)(threshold_minutes=15)
        finally:
            await client.close()

        assert observed == [None, 15]


# ---------------------------------------------------------------------------
# F9 — repo-head staleness header + 404 remediation hint
# ---------------------------------------------------------------------------


class TestRepoHeadStaleness:
    @pytest.mark.asyncio
    async def test_file_read_carries_the_repo_head_line(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/repo/file"):
                return httpx.Response(
                    200, json={"path": "plan.md", "content": "the plan", "size": 8}
                )
            assert request.url.path.endswith("/repo/commits")
            return httpx.Response(
                200,
                json={
                    "total_commits": 1,
                    "commits": [
                        {
                            "sha": "abcdef1234567890",
                            "date": "2026-08-14T10:00:00Z",
                            "message": "phase 2 push\n\nbody",
                        }
                    ],
                },
            )

        client = _client(handler)
        try:
            output = await _invoke("get_job_file", client)(
                job_id=JOB_ID, file_path="plan.md"
            )
        finally:
            await client.close()

        # The E1 'stale' marker: name the exact revision the answer came from.
        assert output.startswith(
            "[repo head: abcdef1 2026-08-14T10:00:00Z — phase 2 push]"
        )
        assert "File: plan.md (ref: HEAD, 8 bytes)" in output

    @pytest.mark.asyncio
    async def test_404_keeps_the_browse_remediation_hint(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/repo/file"):
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json={"total_commits": 0, "commits": []})

        client = _client(handler)
        try:
            output = await _invoke("get_job_file", client)(
                job_id=JOB_ID, file_path="missing.md"
            )
        finally:
            await client.close()

        assert "File 'missing.md' not found at the job branch head" in output
        assert "use list_job_files to browse" in output

    @pytest.mark.asyncio
    async def test_listing_carries_header_and_head_lookup_failure_degrades(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/repo/contents"):
                return httpx.Response(
                    200, json=[{"name": "plan.md", "type": "file", "size": 8}]
                )
            return httpx.Response(503, json={"detail": "gitea down"})

        client = _client(handler)
        try:
            output = await _invoke("list_job_files", client)(job_id=JOB_ID)
        finally:
            await client.close()

        # Best-effort contract: content still returned, header degrades away.
        assert "[file] plan.md" in output
        assert not output.startswith("[repo head")


# ---------------------------------------------------------------------------
# E1 — progress honesty + partial summary
# ---------------------------------------------------------------------------


class TestProgressAndSummaryHonesty:
    @pytest.mark.asyncio
    async def test_progress_renders_liveness_and_never_a_manufactured_percent(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "job_id": JOB_ID,
                    "status": "processing",
                    "progress_percent": None,
                    "eta_seconds": None,
                    "elapsed_seconds": 120,
                    "state": "active",
                    "observed_at": "2026-08-14T12:00:00+00:00",
                    "reasons": ["audit activity 2m ago"],
                    "last_activity_at": "2026-08-14T11:58:00+00:00",
                },
            )

        client = _client(handler)
        try:
            output = await _invoke("get_job_progress", client)(job_id=JOB_ID)
        finally:
            await client.close()

        assert "Liveness: active — last activity 2026-08-14T11:58:00+00:00" in output
        assert "audit activity 2m ago" in output
        assert "Duration: 2m 0s" in output
        assert "0.0%" not in output and "Progress:" not in output.replace(
            f"Progress for job {JOB_ID}", ""
        )

    @pytest.mark.asyncio
    async def test_summary_marks_partial_when_audit_source_fails(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == f"/api/jobs/{JOB_ID}":
                return httpx.Response(200, json=FULL_JOB)
            if path.endswith("/progress"):
                return httpx.Response(
                    200,
                    json={
                        "status": "processing",
                        "state": "active",
                        "reasons": ["audit activity 1m ago"],
                    },
                )
            if path.endswith("/audit"):
                raise httpx.ConnectError("audit db down")
            if path.endswith("/todos"):
                return httpx.Response(200, json={"has_workspace": False})
            if path.endswith("/repo/contents") or path.endswith("/repo/file"):
                return httpx.Response(404, json={"detail": "nope"})
            raise AssertionError(f"unexpected path {path}")

        client = _client(handler)
        try:
            output = await _invoke("get_job_summary", client)(job_id=JOB_ID)
        finally:
            await client.close()

        # Partial distinction: the failed source is named, the rest render.
        assert "PARTIAL summary — unavailable sources: audit_db" in output
        assert "(unavailable: could not connect to the orchestrator)" in output
        assert "Config: worker_base" in output
        assert "Liveness ===" in output or "=== Liveness ===" in output
        # Never render the summary itself as an empty/failed read.
        assert "Status: pending_review" in output or "pending_review" in output
