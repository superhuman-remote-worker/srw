"""Session ↔ job linkage and the cross-tenant hole adding it opened.

``jobs.created_by_thread_id`` is what the completion wake routes on, so how the
value gets there is a security boundary, not bookkeeping:

* On the **internal** path the thread id is authenticated —
  ``_resolve_internal_job_creation_scope`` fetches the thread and 403s when it
  is missing or owned by someone else.
* On the **public** path it was never checked at all. That was harmless while
  the value was a datasource-inheritance hint whose lookup failures are
  swallowed. The moment it is persisted and woken on, an unchecked body field
  lets a caller name a victim's live session and have a completion payload
  POSTed into it — ``/api/input`` on the agent pod has no authentication of any
  kind. Hence the strip, and hence these tests.

Design: knowledge-base/knowledge/features/session_wake_on_job_completion.md.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

USER_ID = str(uuid.uuid4())
THREAD_ID = str(uuid.uuid4())
VICTIM_THREAD_ID = str(uuid.uuid4())
JOB_ID = str(uuid.uuid4())


# --------------------------------------------------------------------------
# The strip
# --------------------------------------------------------------------------


class TestPublicPathStripsThreadId:
    def test_thread_id_is_stripped_from_a_public_payload(self):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.job_create_ingress import (
            strip_public_job_reserved_markers as _strip_public_job_reserved_markers,
        )

        job = JobCreate(description="d", thread_id=VICTIM_THREAD_ID)
        _strip_public_job_reserved_markers(job)

        assert job.thread_id is None, (
            "a cockpit caller must not be able to name a thread — persisting an "
            "unvalidated one turns the wake into cross-tenant message injection"
        )

    def test_thread_id_joins_the_other_system_only_markers(self):
        """It is stripped for the same reason parent_job_id is: derived, never
        submitted."""
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.job_create_ingress import (
            strip_public_job_reserved_markers as _strip_public_job_reserved_markers,
        )

        job = JobCreate(
            description="d",
            thread_id=VICTIM_THREAD_ID,
            parent_job_id=str(uuid.uuid4()),
            creation_order=3,
            worktree_path="/tmp/x",
            delegation_context="ctx",
        )
        _strip_public_job_reserved_markers(job)

        assert (
            job.thread_id,
            job.parent_job_id,
            job.creation_order,
            job.worktree_path,
            job.delegation_context,
        ) == (None, None, None, None, None)

    def test_claim_identity_is_stripped_from_raw_job_context(self):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.job_create_ingress import (
            strip_raw_officer_claim_context as _strip_raw_officer_claim_context,
        )

        job = JobCreate(
            description="d",
            context={
                "ordinary": "preserved",
                "ticket_note_id": "forged",
                "officer_admission": {"ticket_claim_source": "forged"},
                "ticket_ready_at": "2099-01-01T00:00:00Z",
                "ticket_claim_source": "forged",
                "officer_slot": "line",
            },
        )
        # Preserve an independent route-level defense even if a validated
        # request model is mutated before the handler consumes it.
        job.context["evidence_manifest"] = {"source_repository": "victim-private-repo"}
        _strip_raw_officer_claim_context(job)

        assert job.context == {"ordinary": "preserved", "officer_slot": "line"}

    def test_job_create_model_strips_evidence_manifest_at_raw_ingress(self):
        from orchestrator.schemas.job_create import JobCreate

        job = JobCreate(
            description="d",
            context={
                "ordinary": "preserved",
                "evidence_manifest": {
                    "source_repository": "victim-private-repo",
                    "source_revision": "f" * 40,
                },
                "pull_request": {"repo": "victim/private", "number": 1},
                "deliverable_contract_provenance": {"forged": True},
                "required_pr_repositories": ["victim/private"],
            },
        )
        assert job.context == {"ordinary": "preserved"}


# --------------------------------------------------------------------------
# Persistence through create_job
# --------------------------------------------------------------------------


@pytest.fixture
def fake_request():
    return SimpleNamespace(headers={"X-Internal-Key": "secret"}, query_params={})


@pytest.fixture
def linkage_db():
    db = MagicMock()
    db.get_thread = AsyncMock(
        return_value={"id": THREAD_ID, "user_id": USER_ID, "project_id": None}
    )
    db.get_user = AsyncMock(return_value={"id": USER_ID, "is_admin": False})
    db.get_project = AsyncMock(return_value=None)
    db.create_job = AsyncMock(return_value={"id": JOB_ID, "status": "created"})
    db.get_job = AsyncMock(return_value=None)
    db.get_datasource = AsyncMock(return_value=None)
    db.link_datasource_to_job = AsyncMock()
    return db


def _patched(db):
    """Stub every collaborator create_job reaches after scope resolution.

    Deliberately does NOT stub _resolve_internal_job_creation_scope: that is the
    function doing the authentication the strip complements, so the test would
    lose its point if it were mocked away.
    """
    return [
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.application.access.enforce_readiness_gate",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(
                side_effect=lambda _thread_id, **_: (
                    [str(db.get_thread.return_value["project_id"])]
                    if db.get_thread.return_value.get("project_id")
                    else []
                )
            ),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            AsyncMock(side_effect=lambda _thread, project_ids, **_: project_ids),
        ),
        patch(
            "orchestrator.application.jobs.require_job_project_access",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_experts_db_enabled",
            MagicMock(return_value=False),
        ),
        patch(
            "orchestrator.services.job_datasource_selection.inherit_parent_datasource_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.grant_enforcement.enforce_job_create_grants",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()),
        patch(
            "orchestrator.services.subjob_completion.spawn_scholar_subjob",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_dispatcher.trigger_dispatch", MagicMock()),
    ]


async def _create(db, fake_request, body):
    from contextlib import ExitStack

    import orchestrator.security.access as access_module
    from tests._b09_control_seams import create_job

    with ExitStack() as stack:
        stack.enter_context(patch.object(access_module, "_INTERNAL_KEY", "secret"))
        for p in _patched(db):
            stack.enter_context(p)
        return await create_job(fake_request, body)


class TestLinkagePersistence:
    @pytest.mark.asyncio
    async def test_internal_job_context_cannot_bypass_ticket_admission(
        self, linkage_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate

        await _create(
            linkage_db,
            fake_request,
            JobCreate(
                description="internal bypass",
                thread_id=THREAD_ID,
                context={
                    "ordinary": "preserved",
                    "evidence_manifest": {"source_repository": "victim-private-repo"},
                    "ticket_note_id": "forged",
                    "officer_admission": {"ticket_ready_at": "2099-01-01T00:00:00Z"},
                    "ticket_claim_source": "forged",
                    "officer_slot": "line",
                },
            ),
        )

        context = linkage_db.create_job.await_args.kwargs["context"]
        assert context["ordinary"] == "preserved"
        assert context["officer_slot"] == "line"
        assert "evidence_manifest" not in context
        assert "ticket_note_id" not in context
        assert "officer_admission" not in context
        assert "ticket_claim_source" not in context

    @pytest.mark.asyncio
    async def test_public_job_context_cannot_bypass_ticket_admission(
        self, linkage_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate

        fake_request.headers = {}
        with patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value={"id": USER_ID, "is_admin": False}),
        ):
            await _create(
                linkage_db,
                fake_request,
                JobCreate(
                    description="public bypass",
                    context={
                        "ordinary": "preserved",
                        "evidence_manifest": {
                            "source_repository": "victim-private-repo"
                        },
                        "ticket_note_id": "forged",
                        "officer_admission": {
                            "ticket_ready_at": "2099-01-01T00:00:00Z"
                        },
                        "ready_generation_at": "2099-01-01T00:00:00Z",
                        "claim_source": "forged",
                    },
                ),
            )

        context = linkage_db.create_job.await_args.kwargs["context"]
        assert context["ordinary"] == "preserved"
        assert "evidence_manifest" not in context
        assert "ticket_note_id" not in context
        assert "officer_admission" not in context
        assert "ready_generation_at" not in context
        assert "claim_source" not in context

    @pytest.mark.asyncio
    async def test_session_created_job_carries_the_backref_and_opts_into_wake(
        self, linkage_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate

        await _create(
            linkage_db, fake_request, JobCreate(description="d", thread_id=THREAD_ID)
        )

        kwargs = linkage_db.create_job.await_args.kwargs
        assert kwargs["created_by_thread_id"] == THREAD_ID
        # Set server-side, not by the model: an opt-in flag the agent must
        # remember fails silently — it forgets, then never learns the job
        # finished, which is exactly the bug this feature removes.
        assert kwargs["wake_on_complete"] is True

    @pytest.mark.asyncio
    async def test_job_without_a_thread_gets_no_backref(self, linkage_db, fake_request):
        """A cockpit/automation job has nobody to wake."""
        from orchestrator.schemas.job_create import JobCreate

        fake_request.headers = {"X-Internal-Key": "secret", "X-MCP-User-Id": USER_ID}
        parent = str(uuid.uuid4())
        linkage_db.get_job = AsyncMock(
            return_value={"id": parent, "user_id": USER_ID, "project_id": None}
        )

        await _create(
            linkage_db,
            fake_request,
            JobCreate(description="d", parent_job_id=parent),
        )

        kwargs = linkage_db.create_job.await_args.kwargs
        assert kwargs["created_by_thread_id"] is None
        assert kwargs["wake_on_complete"] is False

    @pytest.mark.asyncio
    async def test_worker_child_does_not_inherit_the_backref(
        self, linkage_db, fake_request
    ):
        """A subjob inherits thread scope for datasources but its completion is
        the parent job's business. Waking the session per subjob would turn one
        delegation into a status feed."""
        from orchestrator.schemas.job_create import JobCreate

        parent = str(uuid.uuid4())
        linkage_db.get_job = AsyncMock(
            return_value={"id": parent, "user_id": USER_ID, "project_id": None}
        )

        await _create(
            linkage_db,
            fake_request,
            JobCreate(description="d", thread_id=THREAD_ID, parent_job_id=parent),
        )

        kwargs = linkage_db.create_job.await_args.kwargs
        assert kwargs["created_by_thread_id"] is None
        assert kwargs["wake_on_complete"] is False

    @pytest.mark.asyncio
    async def test_officer_manual_create_uses_authoritative_post_funnel(
        self, linkage_db, fake_request
    ):
        """Manual REST creation and the backlog tick call the same final
        admission-and-INSERT service; the endpoint never performs a second
        unlocked ``create_job`` after checking the slot."""
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.officer_admission import OfficerAdmissionPreparation
        from orchestrator.services.officer_preflight import OfficerPreflightOutcome

        project_id = str(uuid.uuid4())
        linkage_db.get_thread.return_value = {
            "id": THREAD_ID,
            "user_id": USER_ID,
            "project_id": project_id,
            "status": "active",
            "metadata": {
                "config_override": {
                    "officer": {
                        "enabled": True,
                        "slots": {"line": {"count": 1}},
                    }
                }
            },
        }
        linkage_db.get_project.return_value = {
            "id": project_id,
            "default_config_override": None,
            "default_config_name": None,
        }
        linkage_db.get_project_officer_lineage = AsyncMock(return_value=[THREAD_ID])
        preparation = OfficerAdmissionPreparation(
            project_id=project_id,
            thread_id=THREAD_ID,
            requested_slot="line",
            slot_name="line",
            slot_patch={},
            category=None,
            config_fingerprint="proof",
            incarnation=0,
            owner_user_id=USER_ID,
            require_auto_pull=False,
        )
        prepare = AsyncMock(return_value=preparation)
        admitted = {"id": JOB_ID, "status": "paused"}
        admit = AsyncMock(return_value=admitted)
        preflight = AsyncMock(
            return_value=OfficerPreflightOutcome(
                job_id=JOB_ID,
                state="activated",
                activated=True,
                attempted=True,
            )
        )
        linkage_db.get_job.return_value = {"id": JOB_ID, "status": "created"}
        ready_at = datetime(2026, 8, 16, 7, 0, tzinfo=timezone.utc)
        ticket_state = {
            "project_id": project_id,
            "note_id": "feature-proof",
            "note_type": "feature",
            "status": "active",
            "tags": ["ready", "category:executor"],
            "ready_at": ready_at,
        }

        with (
            patch(
                "orchestrator.services.officer_admission.prepare_officer_admission",
                prepare,
            ),
            patch(
                "orchestrator.services.officer_admission.admit_and_create_job", admit
            ),
            patch(
                "orchestrator.services.officer_preflight.ensure_officer_job_activated",
                preflight,
            ),
            patch(
                "orchestrator.services.project_backlog.fetch_ticket_state",
                AsyncMock(return_value=ticket_state),
            ),
        ):
            await _create(
                linkage_db,
                fake_request,
                JobCreate(
                    description="manual officer work",
                    thread_id=THREAD_ID,
                    context={"officer_slot": "line"},
                    ticket="feature-proof",
                ),
            )

        prepare.assert_awaited_once_with(
            linkage_db,
            project_id=project_id,
            thread_id=THREAD_ID,
            requested_slot="line",
            # No config_override on the request, so admission resolves the
            # default workspace backend for the officer it is about to seat.
            requested_config_override=None,
        )
        admit.assert_awaited_once()
        assert admit.await_args.kwargs["preparation"] is preparation
        assert admit.await_args.kwargs["ticket_note_id"] == "feature-proof"
        assert admit.await_args.kwargs["ticket_ready_at"] == ready_at
        assert admit.await_args.kwargs["ticket_claim_source"] == "manual"
        assert admit.await_args.kwargs["strict_provisioning"] is True
        preflight.assert_awaited_once()
        assert preflight.await_args.args[:2] == (linkage_db, admitted)
        assert (
            admit.await_args.kwargs["job_kwargs"]["created_by_thread_id"] == THREAD_ID
        )
        linkage_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ticket_case",
        ["missing", "wrong_project", "not_ready", "ambiguous", "inactive"],
    )
    async def test_manual_ticket_claim_fails_closed_on_untrusted_state(
        self, linkage_db, fake_request, ticket_case
    ):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.officer_admission import OfficerAdmissionPreparation

        project_id = str(uuid.uuid4())
        linkage_db.get_thread.return_value = {
            "id": THREAD_ID,
            "user_id": USER_ID,
            "project_id": project_id,
            "status": "active",
            "metadata": {
                "config_override": {
                    "officer": {
                        "enabled": True,
                        "slots": {"line": {"count": 1}},
                    }
                }
            },
        }
        linkage_db.get_project.return_value = {
            "id": project_id,
            "default_config_override": None,
            "default_config_name": None,
        }
        linkage_db.get_project_officer_lineage = AsyncMock(return_value=[THREAD_ID])
        preparation = OfficerAdmissionPreparation(
            project_id=project_id,
            thread_id=THREAD_ID,
            requested_slot="line",
            slot_name="line",
            slot_patch={},
            category=None,
            config_fingerprint="proof",
            incarnation=0,
            owner_user_id=USER_ID,
            require_auto_pull=False,
        )
        ready_at = datetime(2026, 8, 16, 7, 0, tzinfo=timezone.utc)
        ticket_state = {
            "project_id": project_id,
            "note_id": "feature-proof",
            "note_type": "feature",
            "status": "active",
            "tags": ["ready", "category:executor"],
            "ready_at": ready_at,
        }
        if ticket_case == "missing":
            ticket_state = None
        elif ticket_case == "wrong_project":
            ticket_state["project_id"] = str(uuid.uuid4())
        elif ticket_case == "not_ready":
            ticket_state["ready_at"] = None
        elif ticket_case == "ambiguous":
            ticket_state["tags"] = [
                "ready",
                "category:executor",
                "category:researcher",
            ]
        elif ticket_case == "inactive":
            ticket_state["status"] = "resolved"

        prepare = AsyncMock(return_value=preparation)
        admit = AsyncMock(return_value={"id": JOB_ID, "status": "created"})
        with (
            patch(
                "orchestrator.services.officer_admission.prepare_officer_admission",
                prepare,
            ),
            patch(
                "orchestrator.services.officer_admission.admit_and_create_job", admit
            ),
            patch(
                "orchestrator.services.project_backlog.fetch_ticket_state",
                AsyncMock(return_value=ticket_state),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await _create(
                    linkage_db,
                    fake_request,
                    JobCreate(
                        description="forged manual claim",
                        thread_id=THREAD_ID,
                        context={"officer_slot": "line"},
                        ticket="feature-proof",
                    ),
                )

        assert exc.value.status_code == 409
        admit.assert_not_awaited()

    def test_ready_generation_is_not_model_selectable(self):
        from orchestrator.schemas.job_create import JobCreate

        assert "ready_at" not in JobCreate.model_fields
        assert "ticket_ready_at" not in JobCreate.model_fields
