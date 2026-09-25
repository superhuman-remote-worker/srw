"""POST /resume reports drifted config as 428 and accepts an acknowledgment.

A session's stored config (connector selection, project mounts, capability
grants) can drift out from under it while it is ended — a connector deleted, a
project membership revoked, a grant withdrawn. Before this, ``resume_thread``
ran the plain authorize-or-403 helpers (``_revalidate_thread_project_ids`` /
``_revalidate_thread_datasource_ids``, the latter deleted outright by Task 13
once this feature left it with no production caller), so a single drifted
item made the session permanently un-resumable with no way back (the live
incident behind knowledge-history/done/session_config_drift_resume.md). Now it
reports every drifted item as 428 and accepts an ``acknowledge`` list naming
the ids the caller accepts losing.

This repo has no HTTP test client for these routes. Mirrors the
patch-the-auth-resolver pattern from tests/test_thread_rename.py: call the
endpoint FUNCTION directly, with the auth resolver and ``postgres_db`` patched
via an ExitStack, reusing the conftest fixtures (``user_a``, ``thread_a``,
``fake_db``, ``fake_request``) rather than inventing new ones.

``_thread_config_drift`` — the real drift assembler that runs the Task 1/2/3/5
classifiers — is exercised on its own elsewhere; here it is patched per test so
each case controls exactly what "currently drifted" means without having to
fake out project/datasource/grant classification end to end. The one exception
is ``test_grant_probe_failure_fails_closed``, which runs the real
``_thread_config_drift`` to pin its internal fail-closed handling.
"""

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.services.config_drift import DriftItem

DS_GONE = "d7555d5d-ce46-49e2-b1fa-8235d720badc"
DS_CORRUPT = "c1c1c1c1-c1c1-4c1c-8c1c-c1c1c1c1c1c1"
ENDED_RUNTIME_GENERATION = "77777777-7777-4777-8777-777777777777"
RESUMED_RUNTIME_GENERATION = "88888888-8888-4888-8888-888888888888"


def _discard_background_task(coroutine):
    """Close fire-and-forget work outside this file's drift contract."""
    coroutine.close()
    return SimpleNamespace()


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
    # Past the drift gate, resume_thread reprovisions an agent and
    # provisions/reuses the session's cloud folder — machinery this file has
    # no interest in exercising. Keep it inert and deterministic rather than
    # depending on whatever services.agent_provisioner detects (or not) about
    # the box the tests happen to run on.
    stack.enter_context(
        patch(
            "orchestrator.services.agent_provisioner.agent_provisioner",
            SimpleNamespace(is_available=False),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.persistent_provisioner.persistent_provisioner",
            SimpleNamespace(is_available=False),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.session_provisioner.ensure_session_workspace",
            AsyncMock(),
        )
    )
    stack.enter_context(
        patch(
            "asyncio.create_task",
            side_effect=_discard_background_task,
        )
    )
    db.list_thread_mounts = AsyncMock(return_value=[])

    async def resume_row(thread_id: str) -> bool:
        thread = await db.get_thread(thread_id)
        if (
            not thread
            or thread.get("status") != "ended"
            or thread.get("runtime_retirement_token") is not None
        ):
            return False
        thread["status"] = "created"
        thread["runtime_generation"] = RESUMED_RUNTIME_GENERATION
        thread["agent_id"] = None
        thread["control_admission_agent_id"] = None
        thread["runtime_attach_token"] = None
        thread["runtime_authority_exposed"] = False
        thread["ended_at"] = None
        return True

    db.resume_thread = AsyncMock(side_effect=resume_row)
    return stack


def _ended_thread(thread: dict) -> dict:
    """``thread_a``/``thread_b``, set to the 'ended' state POST /resume
    requires, with the cloud-folder handles already present so the
    (irrelevant, fire-and-forget) late-provision task never gets scheduled."""
    thread["status"] = "ended"
    thread["runtime_generation"] = ENDED_RUNTIME_GENERATION
    thread["runtime_retirement_token"] = None
    thread["metadata"] = {}
    thread["main_cloud_session_handle"] = "existing-handle"
    thread["main_cloud_share_handle"] = "existing-share"
    return thread


def _fake_drift(items: list[DriftItem]) -> AsyncMock:
    """Stand-in for ``_thread_config_drift``: fixed drift for any thread/owner."""
    return AsyncMock(return_value=items)


class TestResumeConfigDrift:
    @pytest.mark.asyncio
    async def test_drift_returns_428_and_does_not_mutate(
        self, user_a, thread_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        drift = [
            DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "KurortEngine")
        ]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(str(thread["id"]), fake_request)

        assert exc.value.status_code == 428
        body = exc.value.detail
        assert body["code"] == "config_drift"
        assert body["detail"] == (
            "Parts of this session's configuration are no longer available"
        )
        assert body["drift"] == [
            {
                "id": f"connector:{DS_GONE}",
                "kind": "connector",
                "reason": "deleted",
                "label": "KurortEngine",
            }
        ]
        # Nothing was mutated: no write ever reached the thread.
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_full_acknowledgment_resumes(
        self, user_a, thread_a, fake_db, fake_request
    ):
        from orchestrator.schemas.thread_lifecycle import ThreadResumeRequest
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        drift = [
            DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "KurortEngine")
        ]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                result = await resume_thread(
                    thread_id,
                    fake_request,
                    ThreadResumeRequest(acknowledge=[f"connector:{DS_GONE}"]),
                )

        assert result == {"status": "created", "thread_id": thread_id}
        fake_db.resume_thread.assert_awaited_once_with(thread_id)
        fake_db.record_thread_config_drift_ack.assert_awaited_once_with(
            thread_id, {f"connector:{DS_GONE}": "deleted"}
        )

    @pytest.mark.asyncio
    async def test_partial_acknowledgment_is_rejected(
        self, user_a, thread_a, fake_db, fake_request
    ):
        from orchestrator.schemas.thread_lifecycle import ThreadResumeRequest
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        drift = [
            DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "KurortEngine"),
            DriftItem("grant:shell_tools", "grant", "revoked", "shell tools"),
        ]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(
                        str(thread["id"]),
                        fake_request,
                        ThreadResumeRequest(acknowledge=[f"connector:{DS_GONE}"]),
                    )

        assert exc.value.status_code == 428
        # Only the still-outstanding item is named, not the acked one.
        assert exc.value.detail["drift"] == [
            {
                "id": f"connector:{DS_GONE}",
                "kind": "connector",
                "reason": "deleted",
                "label": "KurortEngine",
            },
            {
                "id": "grant:shell_tools",
                "kind": "grant",
                "reason": "revoked",
                "label": "shell tools",
            },
        ]
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_superset_acknowledgment_is_accepted(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """An item that recovered between prompt and confirm must not force a
        pointless re-prompt: acknowledging it anyway is harmless — the subset
        rule, not equality."""
        from orchestrator.schemas.thread_lifecycle import ThreadResumeRequest
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        drift = [DriftItem("grant:shell_tools", "grant", "revoked", "shell")]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                result = await resume_thread(
                    thread_id,
                    fake_request,
                    ThreadResumeRequest(
                        acknowledge=["grant:shell_tools", f"connector:{DS_GONE}"]
                    ),
                )

        assert result == {"status": "created", "thread_id": thread_id}
        fake_db.resume_thread.assert_awaited_once_with(thread_id)
        # The stray extra ack id is stored too, harmlessly: only currently
        # drifting items were ever required, but the ack writer's contract is
        # "record what was acknowledged", not "record what mattered".
        fake_db.record_thread_config_drift_ack.assert_awaited_once_with(
            thread_id, {"grant:shell_tools": "revoked"}
        )

    @pytest.mark.asyncio
    async def test_stored_ack_is_honored_without_a_body(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """A: a prior resume's ack is persisted to
        ``metadata.config_drift_ack`` (see ``test_full_acknowledgment_resumes``
        above) but was never read back — so an item the user already
        accepted losing was re-reported as outstanding, and re-acknowledging
        with no body still 428'd. The stored ack must be honored on its own,
        with no ``acknowledge`` in this request at all.
        """
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        thread["metadata"] = {"config_drift_ack": {f"connector:{DS_GONE}": "deleted"}}
        drift = [
            DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "KurortEngine")
        ]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                result = await resume_thread(thread_id, fake_request)

        assert result == {"status": "created", "thread_id": thread_id}
        fake_db.resume_thread.assert_awaited_once_with(thread_id)

    @pytest.mark.asyncio
    async def test_stored_ack_does_not_cover_a_different_new_drift(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """A, converse: a stored ack narrows only the id it names. An item
        that drifts for the FIRST time is in neither the request body nor
        the stored ack, so it must still block with a 428 naming it — the
        union must never let brand-new drift through."""
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        acked_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        thread["metadata"] = {"config_drift_ack": {f"connector:{acked_id}": "deleted"}}
        drift = [
            DriftItem(f"connector:{DS_GONE}", "connector", "deleted", "KurortEngine")
        ]

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift(drift),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(thread_id, fake_request)

        assert exc.value.status_code == 428
        assert exc.value.detail["drift"] == [
            {
                "id": f"connector:{DS_GONE}",
                "kind": "connector",
                "reason": "deleted",
                "label": "KurortEngine",
            }
        ]
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_drift_resumes_exactly_as_before(
        self, user_a, thread_a, fake_db, fake_request
    ):
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                _fake_drift([]),
            ):
                result = await resume_thread(thread_id, fake_request)

        assert result == {"status": "created", "thread_id": thread_id}
        fake_db.resume_thread.assert_awaited_once_with(thread_id)
        # Nothing to acknowledge -> the ack writer is never even called.
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_ended_thread_still_409s(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """Pre-existing behaviour, unchanged: a double-click / already-active
        thread still 409s, and now does so BEFORE drift is even computed."""
        from tests._b09_control_seams import resume_thread

        thread_a["status"] = "created"
        drift_probe = _fake_drift([])

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.thread_resume.thread_config_drift",
                drift_probe,
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(str(thread_a["id"]), fake_request)

        assert exc.value.status_code == 409
        drift_probe.assert_not_awaited()
        fake_db.resume_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_grant_probe_failure_fails_closed(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """The grant probe inside the REAL ``_thread_config_drift`` (not
        patched out here, unlike every other test in this file) must fail
        closed: a transient error while harvesting grant violations is not
        the same as "no grant drift", and reporting it that way would let a
        session resume on an unknown state. Plan §7: "Drift collection
        itself throws -> fail closed — 403, as today. Never auto-proceed
        from an unknown state."

        ``thread_a`` carries no project mounts and no ``datasource_ids``, so
        ``_thread_project_ids`` / ``_classify_thread_project_ids`` /
        ``classify_datasource_selection`` all resolve to empty lists without
        needing any further ``fake_db`` wiring — the ``RuntimeError`` raised
        by the patched ``_resolve_session_config`` is the only thing that can
        reach the handler.
        """
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.session_config_resolution.resolve_session_config",
                AsyncMock(side_effect=RuntimeError("boom")),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(thread_id, fake_request)

        assert exc.value.status_code == 403
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_resuming_another_users_thread_sees_owner_drift(
        self, user_admin, user_a, thread_a, fake_db, fake_request
    ):
        """Regression guard: an admin resuming SOMEONE ELSE's drifted thread
        must get the OWNER's drift, never a silent 200.

        Before the fix, resume_thread computed drift with ``owner=user`` —
        the CALLER — and require_thread_owner lets admins through for
        threads they do not own. Admins pass every classify_* check, so
        ``_thread_config_drift(..., owner=admin)`` found no drift regardless
        of what the real owner's config looked like: resume returned 200
        with NO ack recorded, and the thread flipped ended -> created.
        Attach then enforces against the REAL owner and refuses — and since
        the cockpit only offers the resume dialog while status is 'ended',
        the session became a permanent dead end, strictly worse than the
        blanket 403 this feature replaced.

        Uses the REAL (unmocked) ``_thread_config_drift`` — mocking it would
        hide the exact defect, since a stand-in returns the same drift
        regardless of which owner it is handed. Only the project family is
        exercised (no ``datasource_ids`` on the thread), which is enough to
        discriminate: user_a is not a member of ``project_id`` (revoked),
        while an admin passes ``_classify_thread_project_ids``'s is_admin
        bypass and sees nothing wrong.
        """
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        project_id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

        fake_db.get_user = AsyncMock(return_value=user_a)
        fake_db.get_project = AsyncMock(return_value={"id": project_id})
        fake_db.get_user_role_in_project = AsyncMock(return_value=None)
        fake_db.get_datasource_tombstones = AsyncMock(return_value={})

        with _patch_caller_and_db(user_admin, fake_db):
            with (
                patch(
                    "orchestrator.services.thread_mount_rows.thread_project_ids",
                    AsyncMock(return_value=[project_id]),
                ),
                patch(
                    "orchestrator.services.session_config_resolution.resolve_session_config",
                    AsyncMock(return_value=None),
                ),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(thread_id, fake_request)

        assert exc.value.status_code == 428
        assert exc.value.detail["drift"] == [
            {
                "id": f"project:{project_id}",
                "kind": "project",
                "reason": "revoked",
                "label": "a project you no longer have access to",
            }
        ]
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_corrupt_revision_refuses_at_resume_with_403(
        self, user_a, thread_a, fake_db, fake_request
    ):
        """B: ``workspace_tier`` / ``corrupt_revision`` are deliberately
        excluded from ``ACKNOWLEDGEABLE_REASONS``, so ``collect_config_drift``
        never turns either into a ``DriftItem`` — without a dedicated check,
        resume would see NO drift, return 200, and flip the thread to
        'created'. Attach then denies: the silent "Connecting..." hang this
        whole feature exists to remove. Uses the REAL ``_thread_config_drift``
        (only the grant probe is stubbed inert) so the blocking check is
        genuinely exercised, not assumed away by a mock.
        """
        from tests._b09_control_seams import resume_thread

        thread = _ended_thread(thread_a)
        thread_id = str(thread["id"])
        thread["metadata"] = {"datasource_ids": [DS_CORRUPT]}

        fake_db.get_user = AsyncMock(return_value=user_a)
        fake_db.get_datasource_tombstones = AsyncMock(return_value={})
        fake_db.get_datasource_policy_rows = AsyncMock(
            return_value=[
                {
                    "id": DS_CORRUPT,
                    "type": "generic",
                    "scope_mode": "all",
                    "is_global": True,
                    "created_by": None,
                    "policy_revision": 0,
                }
            ]
        )

        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.session_config_resolution.resolve_session_config",
                AsyncMock(return_value=None),
            ):
                with pytest.raises(HTTPException) as exc:
                    await resume_thread(thread_id, fake_request)

        assert exc.value.status_code == 403
        # Non-enumerating: the response must not name which item is invalid.
        assert DS_CORRUPT not in str(exc.value.detail)
        fake_db.resume_thread.assert_not_awaited()
        fake_db.record_thread_config_drift_ack.assert_not_awaited()
