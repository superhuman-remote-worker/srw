"""User-facing rename for persistent threads — PATCH /api/persistent/threads/{id}.

The title was previously settable only at creation (and auto-generated once by
the LLM after the first turn); there was no way for a user to rename a session.
This covers the new route: the happy path (owner renames, title trimmed,
``update_thread_title`` called), input validation (empty / whitespace /
over-length → 400 with no DB write), and that the ``require_thread_owner`` gate
is wired (cross-user → 403, missing → 404).

Mirrors the patch-the-auth-resolver pattern from tests/test_thread_access.py.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.routers.thread_session import ThreadSessionDependencies, update_thread
from orchestrator.schemas.thread_admission import ThreadUpdateRequest
from orchestrator.security.access import require_thread_owner


def _unused(name: str):
    async def _fail(*_args, **_kwargs):
        raise AssertionError(f"rename must not reach {name}")

    return _fail


def _dependencies(db) -> ThreadSessionDependencies:
    """The rename route's collaborators: the store and the REAL owner gate."""
    return ThreadSessionDependencies(
        store=db,
        require_thread_owner=require_thread_owner,
        require_approved_user=_unused("require_approved_user"),
        resolve_cloud_session_url=_unused("resolve_cloud_session_url"),
        resolve_session_config=_unused("resolve_session_config"),
        enforce_session_create_grants=_unused("enforce_session_create_grants"),
        tool_view=SimpleNamespace(),
    )


def _patch_caller(user: dict):
    """``require_thread_owner`` resolves the caller through its own module."""
    return patch(
        "orchestrator.security.access.require_approved_user",
        AsyncMock(return_value=user),
    )


class TestUpdateThread:
    @pytest.mark.asyncio
    async def test_owner_can_rename(self, user_a, thread_a, fake_db, fake_request):
        with _patch_caller(user_a):
            result = await update_thread(
                str(thread_a["id"]),
                ThreadUpdateRequest(title="My renamed session"),
                fake_request,
                dependencies=_dependencies(fake_db),
            )
        assert result["status"] == "updated"
        assert result["title"] == "My renamed session"
        fake_db.update_thread_title.assert_awaited_once_with(
            str(thread_a["id"]), "My renamed session"
        )

    @pytest.mark.asyncio
    async def test_title_is_trimmed(self, user_a, thread_a, fake_db, fake_request):
        with _patch_caller(user_a):
            result = await update_thread(
                str(thread_a["id"]),
                ThreadUpdateRequest(title="  spaced out  "),
                fake_request,
                dependencies=_dependencies(fake_db),
            )
        assert result["title"] == "spaced out"
        fake_db.update_thread_title.assert_awaited_once_with(
            str(thread_a["id"]), "spaced out"
        )

    @pytest.mark.asyncio
    async def test_empty_title_rejected(self, user_a, thread_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await update_thread(
                    str(thread_a["id"]),
                    ThreadUpdateRequest(title="   "),
                    fake_request,
                    dependencies=_dependencies(fake_db),
                )
        assert exc.value.status_code == 400
        fake_db.update_thread_title.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_too_long_title_rejected(
        self, user_a, thread_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await update_thread(
                    str(thread_a["id"]),
                    ThreadUpdateRequest(title="x" * 201),
                    fake_request,
                    dependencies=_dependencies(fake_db),
                )
        assert exc.value.status_code == 400
        fake_db.update_thread_title.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cross_user_rename_forbidden(
        self, user_b, thread_a, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_thread(
                    str(thread_a["id"]),
                    ThreadUpdateRequest(title="hijack"),
                    fake_request,
                    dependencies=_dependencies(fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.update_thread_title.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_thread_404(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await update_thread(
                    "00000000-0000-0000-0000-000000000999",
                    ThreadUpdateRequest(title="ghost"),
                    fake_request,
                    dependencies=_dependencies(fake_db),
                )
        assert exc.value.status_code == 404
        fake_db.update_thread_title.assert_not_awaited()
