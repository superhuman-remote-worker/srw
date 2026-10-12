"""G3 — multi-tenancy gates on sources + sudo list/get.

**Sources** (`list_sources`, `get_source_detail`) are reachable via the
M:N ``job_sources`` table. G3 visibility:

* ``list_sources?job_id=...`` → ``require_job_access`` on that job.
* ``list_sources`` without ``job_id`` → admin-only (cross-job source
  enumeration would require a vector_db ⇆ postgres_db join we
  deliberately avoid).
* ``get_source_detail`` → caller must be able to access at least one
  job linked to the source.

**Sudo** (`list_sudo_requests`, `get_sudo_request`):

* ``list_sudo_requests?job_id=...`` → ``require_job_access`` on that job.
* ``list_sudo_requests`` without ``job_id`` → admins see all; non-admins
  receive only requests whose underlying job they can access (post-fetch
  filter).
* ``get_sudo_request`` → caller must be able to access the underlying
  job (admin bypass unless MCP project-scoped).

The mutation endpoints (approve/deny/upgrade/resume-without-vm) were
already gated by H4 via ``require_sudo_request_authority`` — not in
scope here.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from orchestrator.application import projects as projects_composition


def _patch_caller_and_db(user: dict, db):
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


def _vector_db_with_source(source_row, job_ids):
    """Patch `main.vector_db.acquire()` to return a fixed source + job list."""
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=source_row)
    conn.fetch = AsyncMock(return_value=[{"job_id": jid} for jid in job_ids])
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=None)
    vector_db = MagicMock()
    vector_db.acquire = MagicMock(return_value=ctx)
    return vector_db


# =============================================================================
# user_can_access_any_job — helper
# =============================================================================


class TestUserCanAccessAnyJob:
    @pytest.mark.asyncio
    async def test_admin_no_scope_short_circuit(self, user_admin, fake_db):
        from orchestrator.security.access import user_can_access_any_job

        # Admin should pass without ever calling the DB.
        fake_db.get_job = AsyncMock(
            side_effect=AssertionError("get_job called for admin short-circuit")
        )
        assert await user_can_access_any_job(user_admin, fake_db, []) is True

    @pytest.mark.asyncio
    async def test_empty_list_non_admin_false(self, user_a, fake_db):
        from orchestrator.security.access import user_can_access_any_job

        assert await user_can_access_any_job(user_a, fake_db, []) is False

    @pytest.mark.asyncio
    async def test_first_match_passes(self, user_a, job_a, fake_db):
        from orchestrator.security.access import user_can_access_any_job

        result = await user_can_access_any_job(user_a, fake_db, [str(job_a["id"])])
        assert result is True

    @pytest.mark.asyncio
    async def test_no_match_false(self, user_b, job_a, fake_db):
        from orchestrator.security.access import user_can_access_any_job

        # user_b has no access to job_a (different project, not owner).
        result = await user_can_access_any_job(user_b, fake_db, [str(job_a["id"])])
        assert result is False


# =============================================================================
# list_sources
# =============================================================================


class TestListSources:
    @pytest.mark.asyncio
    async def test_with_job_id_gated_by_require_job_access(
        self, user_b, job_a, fake_db, fake_request
    ):
        import orchestrator.main
        from orchestrator.routers.citations import list_sources

        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.app.state.resources.vector_db",
                MagicMock(side_effect=AssertionError("vector_db hit past gate")),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_sources(
                    fake_request,
                    job_id=str(job_a["id"]),
                    type=None,
                    limit=50,
                    offset=0,
                    dependencies=projects_composition.citations_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_without_job_id_non_admin_403(self, user_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import list_sources

        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.main.app.state.resources.vector_db",
                MagicMock(side_effect=AssertionError("vector_db hit past gate")),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_sources(
                    fake_request,
                    job_id=None,
                    type=None,
                    limit=50,
                    offset=0,
                    dependencies=projects_composition.citations_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_without_job_id_admin_passes(self, user_admin, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import list_sources

        # Mock vector_db.acquire() to return zero rows.
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value={"total": 0})
        conn.fetch = AsyncMock(return_value=[])
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=None)
        vector_db = MagicMock()
        vector_db.acquire = MagicMock(return_value=ctx)

        with (
            _patch_caller_and_db(user_admin, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            result = await list_sources(
                fake_request,
                job_id=None,
                type=None,
                limit=50,
                offset=0,
                dependencies=projects_composition.citations_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result == {"sources": [], "total": 0}


# =============================================================================
# get_source_detail
# =============================================================================


class TestGetSourceDetail:
    @pytest.mark.asyncio
    async def test_visible_via_owned_job(self, user_a, job_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import get_source_detail

        source_row = {
            "id": 1,
            "type": "doc",
            "identifier": "x",
            "name": "x",
            "version": None,
            "content": "abc",
            "content_hash": "h",
            "metadata": {},
            "created_at": None,
            "full_content_length": 3,
        }
        vector_db = _vector_db_with_source(source_row, [str(job_a["id"])])
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            result = await get_source_detail(
                fake_request,
                1,
                content_limit=0,
                dependencies=projects_composition.citations_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["id"] == 1
        assert result["job_ids"] == [str(job_a["id"])]

    @pytest.mark.asyncio
    async def test_no_accessible_job_403(self, user_b, job_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import get_source_detail

        source_row = {
            "id": 1,
            "type": "doc",
            "identifier": "x",
            "name": "x",
            "version": None,
            "content": "abc",
            "content_hash": "h",
            "metadata": {},
            "created_at": None,
            "full_content_length": 3,
        }
        vector_db = _vector_db_with_source(source_row, [str(job_a["id"])])
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_source_detail(
                    fake_request,
                    1,
                    content_limit=0,
                    dependencies=projects_composition.citations_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_bypass(self, user_admin, job_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import get_source_detail

        source_row = {
            "id": 1,
            "type": "doc",
            "identifier": "x",
            "name": "x",
            "version": None,
            "content": "abc",
            "content_hash": "h",
            "metadata": {},
            "created_at": None,
            "full_content_length": 3,
        }
        vector_db = _vector_db_with_source(source_row, [str(job_a["id"])])
        with (
            _patch_caller_and_db(user_admin, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            result = await get_source_detail(
                fake_request,
                1,
                content_limit=0,
                dependencies=projects_composition.citations_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["id"] == 1

    @pytest.mark.asyncio
    async def test_orphan_source_no_jobs_403_for_non_admin(
        self, user_a, fake_db, fake_request
    ):
        """Source with zero linked jobs is admin-only."""
        import orchestrator.main
        from orchestrator.routers.citations import get_source_detail

        source_row = {
            "id": 1,
            "type": "doc",
            "identifier": "x",
            "name": "x",
            "version": None,
            "content": "abc",
            "content_hash": "h",
            "metadata": {},
            "created_at": None,
            "full_content_length": 3,
        }
        vector_db = _vector_db_with_source(source_row, [])
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_source_detail(
                    fake_request,
                    1,
                    content_limit=0,
                    dependencies=projects_composition.citations_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_source_404(self, user_a, fake_db, fake_request):
        import orchestrator.main
        from orchestrator.routers.citations import get_source_detail

        vector_db = _vector_db_with_source(None, [])
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.main.app.state.resources.vector_db", vector_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_source_detail(
                    fake_request,
                    999,
                    content_limit=0,
                    dependencies=projects_composition.citations_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 404


# =============================================================================
# list_sudo_requests
# =============================================================================


class TestListSudoRequests:
    @pytest.mark.asyncio
    async def test_with_job_id_gated_by_require_job_access(
        self, user_b, job_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import list_sudo_requests

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.list_requests = AsyncMock(
            side_effect=AssertionError("sudo_gate.list called past gate")
        )
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_sudo_requests(
                    fake_request,
                    job_id=str(job_a["id"]),
                    status=None,
                    request_type=None,
                    limit=50,
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_with_job_id_owner_passes(self, user_a, job_a, fake_db, fake_request):
        from tests._b09_control_seams import list_sudo_requests

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.list_requests = AsyncMock(return_value=[{"id": "r1"}])
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            result = await list_sudo_requests(
                fake_request,
                job_id=str(job_a["id"]),
                status=None,
                request_type=None,
                limit=50,
            )
        assert result == [{"id": "r1"}]

    @pytest.mark.asyncio
    async def test_no_job_id_admin_sees_all(
        self, user_admin, job_a, job_b, fake_db, fake_request
    ):
        from tests._b09_control_seams import list_sudo_requests

        all_rows = [
            {"id": "r1", "job_id": str(job_a["id"])},
            {"id": "r2", "job_id": str(job_b["id"])},
        ]
        fake_sudo_gate = MagicMock()
        fake_sudo_gate.list_requests = AsyncMock(return_value=all_rows)
        with (
            _patch_caller_and_db(user_admin, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            result = await list_sudo_requests(
                fake_request,
                job_id=None,
                status=None,
                request_type=None,
                limit=50,
            )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_no_job_id_non_admin_filters_to_visible(
        self, user_a, job_a, job_b, fake_db, fake_request
    ):
        from tests._b09_control_seams import list_sudo_requests

        all_rows = [
            {"id": "r1", "job_id": str(job_a["id"])},  # accessible
            {"id": "r2", "job_id": str(job_b["id"])},  # not accessible
        ]
        fake_sudo_gate = MagicMock()
        fake_sudo_gate.list_requests = AsyncMock(return_value=all_rows)
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            result = await list_sudo_requests(
                fake_request,
                job_id=None,
                status=None,
                request_type=None,
                limit=50,
            )
        assert len(result) == 1
        assert result[0]["id"] == "r1"


# =============================================================================
# get_sudo_request
# =============================================================================


class TestGetSudoRequest:
    @pytest.mark.asyncio
    async def test_missing_404(self, user_a, fake_db, fake_request):
        from tests._b09_control_seams import get_sudo_request

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.get_request = AsyncMock(return_value=None)
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_sudo_request(fake_request, "missing")
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, job_a, fake_db, fake_request):
        from tests._b09_control_seams import get_sudo_request

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.get_request = AsyncMock(
            return_value={"id": "r1", "job_id": str(job_a["id"])}
        )
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            result = await get_sudo_request(fake_request, "r1")
        assert result["id"] == "r1"

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, job_a, fake_db, fake_request):
        from tests._b09_control_seams import get_sudo_request

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.get_request = AsyncMock(
            return_value={"id": "r1", "job_id": str(job_a["id"])}
        )
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_sudo_request(fake_request, "r1")
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_bypass(self, user_admin, job_a, fake_db, fake_request):
        from tests._b09_control_seams import get_sudo_request

        fake_sudo_gate = MagicMock()
        fake_sudo_gate.get_request = AsyncMock(
            return_value={"id": "r1", "job_id": str(job_a["id"])}
        )
        with (
            _patch_caller_and_db(user_admin, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            result = await get_sudo_request(fake_request, "r1")
        assert result["id"] == "r1"

    @pytest.mark.asyncio
    async def test_mcp_project_mismatch_403(
        self, user_admin, job_a, project_b, fake_db, fake_request
    ):
        from tests._b09_control_seams import get_sudo_request

        scoped = _scoped(user_admin, f"project:{project_b['id']}")
        fake_sudo_gate = MagicMock()
        fake_sudo_gate.get_request = AsyncMock(
            return_value={"id": "r1", "job_id": str(job_a["id"])}
        )
        with (
            _patch_caller_and_db(scoped, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", fake_sudo_gate),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_sudo_request(fake_request, "r1")
        assert exc.value.status_code == 403
