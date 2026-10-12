"""``ssh_handle`` reaching the browser (workspace-ssh-access-client, task 2).

Two separate claims, per the task-2 brief's CONTROLLER CORRECTIONS (C1/C2):

C1: ``thread_projection.redact_thread_metadata`` is the single funnel both
thread endpoints (list + single) run every row through before it leaves over
REST.
It does not allow-list columns — it pops a fixed set of internal
runtime/retirement keys and passes the rest of the row through unchanged —
so a plain ``ssh_handle`` column should already survive it with zero code
change. This is proven directly rather than assumed; if it turns out some
future refactor turns this into an allow-list, these tests are the ones
that must start failing.

C2: the actual gap is mint-on-view. Threads created before migration 0202
have ``ssh_handle IS NULL`` and stay that way forever unless something
mints one lazily. ``GET /api/persistent/threads/{thread_id}`` is the one
place a controller ruled that should happen — it must call
``PostgresDB.ensure_thread_ssh_handle`` when the stored value is falsy, so
an old thread gets a handle on first view instead of showing an empty SSH
panel forever. ``GET /api/persistent/threads`` (the list endpoint)
deliberately does NOT do this — minting up to 50 handles as a side effect
of rendering a list is write amplification nobody asked for — so only the
single-thread path is covered here.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.routers.thread_session import ThreadSessionDependencies, get_thread
from orchestrator.security.access import require_thread_owner
from orchestrator.services.thread_projection import redact_thread_metadata


# ---------------------------------------------------------------------------
# C1 -- redact_thread_metadata already passes ssh_handle through unchanged
# ---------------------------------------------------------------------------


class TestRedactThreadMetadataCarriesSshHandle:
    def test_present_handle_survives_redaction(self):
        out = redact_thread_metadata({"id": "t", "ssh_handle": "s-7f3a91c2"})
        assert out["ssh_handle"] == "s-7f3a91c2"

    def test_null_handle_survives_as_none(self):
        """A real ``SELECT *`` row for a thread predating 0202 carries the
        column with a NULL value, not an absent key."""
        out = redact_thread_metadata({"id": "t", "ssh_handle": None})
        assert out["ssh_handle"] is None


# ---------------------------------------------------------------------------
# C2 -- GET /api/persistent/threads/{thread_id} mints a handle on first view
# ---------------------------------------------------------------------------


def _unused(name):
    async def _fail(*_args, **_kwargs):
        raise AssertionError(f"the thread view must not reach {name}")

    return _fail


def _dependencies(db):
    """The endpoint's collaborators: the store it reads the thread and mounts
    from, and the REAL ``require_thread_owner`` (security/access.py), which
    resolves the caller via its *own* module's ``require_approved_user`` —
    patched by :func:`_patch_caller`. ``resolve_cloud_session_url`` is stubbed
    so these tests don't also have to wire up mount rows."""
    return ThreadSessionDependencies(
        store=db,
        require_thread_owner=require_thread_owner,
        require_approved_user=_unused("require_approved_user"),
        resolve_cloud_session_url=MagicMock(return_value=None),
        resolve_session_config=_unused("resolve_session_config"),
        enforce_session_create_grants=_unused("enforce_session_create_grants"),
        tool_view=SimpleNamespace(),
    )


def _patch_caller(user):
    return patch(
        "orchestrator.security.access.require_approved_user",
        AsyncMock(return_value=user),
    )


class TestGetThreadMintsSshHandleOnView:
    @pytest.mark.asyncio
    async def test_mints_a_handle_when_missing(self, user_a, thread_a, fake_db):
        thread_a["ssh_handle"] = None
        fake_db.ensure_thread_ssh_handle = AsyncMock(return_value="s-newlymnt")

        with _patch_caller(user_a):
            result = await get_thread(
                str(thread_a["id"]), MagicMock(), dependencies=_dependencies(fake_db)
            )

        assert result["ssh_handle"] == "s-newlymnt"
        fake_db.ensure_thread_ssh_handle.assert_awaited_once_with(str(thread_a["id"]))

    @pytest.mark.asyncio
    async def test_does_not_mint_when_a_handle_already_exists(
        self, user_a, thread_a, fake_db
    ):
        """Minting is a write; a thread that already has a handle must not
        pay for one on every view."""
        thread_a["ssh_handle"] = "s-7f3a91c2"
        fake_db.ensure_thread_ssh_handle = AsyncMock(
            side_effect=AssertionError("must not mint: a handle already exists")
        )

        with _patch_caller(user_a):
            result = await get_thread(
                str(thread_a["id"]), MagicMock(), dependencies=_dependencies(fake_db)
            )

        assert result["ssh_handle"] == "s-7f3a91c2"
        fake_db.ensure_thread_ssh_handle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mint_failure_degrades_to_a_null_handle_instead_of_500(
        self, user_a, thread_a, fake_db
    ):
        """M-1 (final fix wave): the mint is a WRITE on an otherwise
        read-only view. A read-only replica or a full disk -- this
        deployment has actually had one -- must not turn the whole thread
        view into a 500 for the sake of one SSH-panel field."""
        thread_a["ssh_handle"] = None
        fake_db.ensure_thread_ssh_handle = AsyncMock(
            side_effect=RuntimeError("could not extend file: No space left on device")
        )

        with _patch_caller(user_a):
            result = await get_thread(
                str(thread_a["id"]), MagicMock(), dependencies=_dependencies(fake_db)
            )

        assert result["ssh_handle"] is None
        fake_db.ensure_thread_ssh_handle.assert_awaited_once_with(str(thread_a["id"]))
