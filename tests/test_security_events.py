"""M1.B #4 — denied-access security event log.

Every 403 raised by a ``security/access.py`` gate (plus ``_require_admin``
and the IDE proxy denials in main.py) must leave a trace: a structured
WARNING line + a best-effort ``security_events`` row. Design in
``knowledge-base/knowledge/features/security_event_log.md``.

What's covered here:

* the deny paths write an event with the right shape (resource_type /
  resource_id / detail / caller identity)
* the happy path and the 404 path write nothing (404 = resource doesn't
  exist; only "exists but foreign" is a probe signal)
* a failing DB write never masks the 403 (audit outage must not become
  a 500, and must not let the request through)
* view-as shadow enrichment (``view_as`` / ``real_is_admin``)
* ``_request_meta`` extraction (X-Forwarded-For first hop, MagicMock
  paranoia)
* the admin read endpoint (gated, filtered, bad ``since`` → 400)

Tests share the 3-user / 2-project fixture set from ``conftest.py``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.security.access import (
    _request_meta,
    log_security_event,
    require_job_access,
    require_thread_owner,
)
from orchestrator.application import administration as administration_composition


def _patch_auth(user: dict):
    """Route ``require_approved_user`` inside security.access to ``user``."""
    return patch(
        "orchestrator.security.access.require_approved_user",
        AsyncMock(return_value=user),
    )


def _event_kwargs(db) -> dict:
    """The kwargs of the single record_security_event call."""
    assert db.record_security_event.await_count == 1
    return db.record_security_event.await_args.kwargs


class _Req:
    """Request stand-in with real string attributes (vs the MagicMock one)."""

    def __init__(self, headers=None, method="GET", path="/api/x", host="10.0.0.9"):
        self.method = method
        self.headers = headers or {}
        self.url = SimpleNamespace(path=path)
        self.client = SimpleNamespace(host=host)


# =============================================================================
# Deny paths write events
# =============================================================================


class TestDenyWritesEvent:
    @pytest.mark.asyncio
    async def test_cross_user_job_403_writes_event(self, user_a, job_b, fake_db):
        req = _Req(path=f"/api/jobs/{job_b['id']}")
        with _patch_auth(user_a):
            with pytest.raises(HTTPException) as exc:
                await require_job_access(req, fake_db, str(job_b["id"]))
        assert exc.value.status_code == 403
        kwargs = _event_kwargs(fake_db)
        assert kwargs["event_type"] == "access_denied"
        assert kwargs["resource_type"] == "job"
        assert kwargs["resource_id"] == str(job_b["id"])
        assert kwargs["user_id"] == str(user_a["id"])
        assert kwargs["auth_method"] == "cookie"
        assert kwargs["detail"] == "Not authorized to access this job"
        assert kwargs["method"] == "GET"
        assert kwargs["path"] == f"/api/jobs/{job_b['id']}"
        assert kwargs["real_is_admin"] is False
        assert kwargs["view_as"] is False

    @pytest.mark.asyncio
    async def test_cross_user_thread_403_writes_event(
        self, user_a, thread_b, fake_db, fake_request
    ):
        with _patch_auth(user_a):
            with pytest.raises(HTTPException):
                await require_thread_owner(fake_request, fake_db, str(thread_b["id"]))
        kwargs = _event_kwargs(fake_db)
        assert kwargs["resource_type"] == "thread"
        assert kwargs["detail"] == "Not your thread"

    @pytest.mark.asyncio
    async def test_scope_denial_writes_event(
        self, user_a, job_a, fake_db, fake_request
    ):
        """A project-scoped MCP token denied on scope still leaves a trace."""
        scoped = dict(user_a)
        scoped["scopes"] = ["project:99999999-9999-9999-9999-999999999999"]
        scoped["auth_method"] = "mcp"
        with _patch_auth(scoped):
            with pytest.raises(HTTPException) as exc:
                await require_job_access(fake_request, fake_db, str(job_a["id"]))
        assert exc.value.status_code == 403
        kwargs = _event_kwargs(fake_db)
        assert kwargs["detail"] == "Access denied by MCP token scope"
        assert kwargs["auth_method"] == "mcp"

    @pytest.mark.asyncio
    async def test_view_as_shadow_recorded(
        self, user_admin, job_b, fake_db, fake_request
    ):
        """Shadowed admin (view-as on) denied → view_as=True, real_is_admin=True."""
        shadowed = dict(user_admin)
        shadowed["is_admin"] = False  # the shadow
        shadowed["real_is_admin"] = True
        with _patch_auth(shadowed):
            with pytest.raises(HTTPException) as exc:
                await require_job_access(fake_request, fake_db, str(job_b["id"]))
        assert exc.value.status_code == 403
        kwargs = _event_kwargs(fake_db)
        assert kwargs["view_as"] is True
        assert kwargs["real_is_admin"] is True

    @pytest.mark.asyncio
    async def test_warning_line_emitted(
        self, user_a, job_b, fake_db, fake_request, caplog
    ):
        """The structured log line fires (it's the DB-outage fallback)."""
        with (
            _patch_auth(user_a),
            caplog.at_level("WARNING", logger="orchestrator.security.access"),
        ):
            with pytest.raises(HTTPException):
                await require_job_access(fake_request, fake_db, str(job_b["id"]))
        assert any("security-event access_denied" in r.message for r in caplog.records)


# =============================================================================
# Non-deny paths write nothing
# =============================================================================


class TestNoEventOnNonDeny:
    @pytest.mark.asyncio
    async def test_owner_success_writes_nothing(
        self, user_a, job_a, fake_db, fake_request
    ):
        with _patch_auth(user_a):
            _user, job = await require_job_access(
                fake_request, fake_db, str(job_a["id"])
            )
        assert job["id"] == job_a["id"]
        fake_db.record_security_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_404_writes_nothing(self, user_a, fake_db, fake_request):
        """Unknown resource = typo/stale UI, not a probe — keep the table quiet."""
        with _patch_auth(user_a):
            with pytest.raises(HTTPException) as exc:
                await require_job_access(
                    fake_request, fake_db, "00000000-0000-0000-0000-00000000dead"
                )
        assert exc.value.status_code == 404
        fake_db.record_security_event.assert_not_awaited()


# =============================================================================
# Audit failure never blocks the deny
# =============================================================================


class TestWriteFailureContainment:
    @pytest.mark.asyncio
    async def test_db_write_failure_does_not_mask_403(
        self, user_a, job_b, fake_db, fake_request, caplog
    ):
        fake_db.record_security_event = AsyncMock(side_effect=RuntimeError("pool down"))
        with (
            _patch_auth(user_a),
            caplog.at_level("ERROR", logger="orchestrator.security.access"),
        ):
            with pytest.raises(HTTPException) as exc:
                await require_job_access(fake_request, fake_db, str(job_b["id"]))
        assert exc.value.status_code == 403
        assert any(
            "security-event DB write failed" in r.message for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_none_db_is_tolerated(self, user_a):
        """Direct helper call with db=None only logs — never raises."""
        await log_security_event(None, resource_type="job", user=user_a)


# =============================================================================
# _request_meta extraction
# =============================================================================


class TestRequestMeta:
    def test_forwarded_for_first_hop_wins(self):
        req = _Req(headers={"x-forwarded-for": "203.0.113.7, 10.42.0.1"})
        method, path, ip = _request_meta(req)
        assert (method, path, ip) == ("GET", "/api/x", "203.0.113.7")

    def test_falls_back_to_client_host(self):
        method, path, ip = _request_meta(_Req())
        assert ip == "10.0.0.9"

    def test_bare_magicmock_degrades_to_none(self):
        method, path, ip = _request_meta(MagicMock())
        assert (method, path, ip) == (None, None, None)

    def test_none_request(self):
        assert _request_meta(None) == (None, None, None)


# =============================================================================
# Admin read endpoint
# =============================================================================


def _patch_main(user: dict, db):
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


class TestAdminEndpoint:
    @pytest.mark.asyncio
    async def test_admin_lists_events(self, user_admin, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.user_administration import (
            admin_list_security_events,
        )

        fake_db.list_security_events = AsyncMock(
            return_value=[{"event_type": "access_denied"}]
        )
        with _patch_main(user_admin, fake_db):
            out = await admin_list_security_events(
                fake_request,
                limit=10,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert out["count"] == 1
        fake_db.list_security_events.assert_awaited_once_with(
            limit=10, user_id=None, event_type=None, since=None
        )

    @pytest.mark.asyncio
    async def test_non_admin_denied_and_audited(self, user_a, fake_db, fake_request):
        """The gate guarding the audit log itself writes an admin_denied event."""
        import orchestrator.main
        from orchestrator.routers.user_administration import (
            admin_list_security_events,
        )

        with _patch_main(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await admin_list_security_events(
                    fake_request,
                    dependencies=administration_composition.user_administration_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403
        kwargs = _event_kwargs(fake_db)
        assert kwargs["event_type"] == "admin_denied"
        assert kwargs["resource_type"] == "admin_endpoint"

    @pytest.mark.asyncio
    async def test_bad_since_is_400(self, user_admin, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.user_administration import (
            admin_list_security_events,
        )

        with _patch_main(user_admin, fake_db):
            with pytest.raises(HTTPException) as exc:
                await admin_list_security_events(
                    fake_request,
                    since="not-a-date",
                    dependencies=administration_composition.user_administration_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 400
