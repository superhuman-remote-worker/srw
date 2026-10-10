"""A job's project-folder copy, told truthfully (main cloud slice 6.3).

Pinned here: what admission and provisioning decide from the attached
``cloud_folder_checkout`` and ``cloud_outbox`` rows (``context.cloud_checkout``,
``context.cloud_connectors``, the copy's ``seeding`` hold), what the task brief
says about the copy and the outbox, what the seed records about what it
copied, which jobs capture a diff and what the capture found, and that a
critic's approval leaves a captured diff waiting for its owner.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.core.deliverables import format_cloud_connectors_block
from orchestrator.services import job_cloud_checkout as checkout
from orchestrator.services.cloud import ProjectFolderEntry
from orchestrator.services.job_cloud_baseline import (
    capture_checkout_diff,
    seed_project_folder_baseline,
)
from shared.job_cloud_connectors import (
    checkout_entry,
    connector_entry,
    outbox_entry,
    parse_cloud_connectors,
)

PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
OTHER_PROJECT = "00000000-0000-0000-0000-0000000000b2"
USER_ID = "00000000-0000-0000-0000-0000000000a1"
OTHER_USER = "00000000-0000-0000-0000-0000000000a2"
CHECKOUT_ID = "00000000-0000-0000-0000-0000000000c1"
OUTBOX_ID = "00000000-0000-0000-0000-0000000000c2"
JOB_ID = "00000000-0000-0000-0000-0000000000e1"

PROJECT = {
    "id": PROJECT_ID,
    "name": "Hand Book",
    "main_cloud_folder_handle": "7",
    "main_cloud_backend": "nextcloud",
}


def _checkout_row(**changes):
    row = {
        "id": CHECKOUT_ID,
        "name": "Project folder copy",
        "type": "cloud_folder_checkout",
        "config": {"project_id": PROJECT_ID},
        "managed_key": f"project-cloud-checkout:{PROJECT_ID}",
        "read_only": None,
        "is_global": False,
        "created_by": None,
        "project_read_only": None,
    }
    row.update(changes)
    return row


def _outbox_row(user=USER_ID, **changes):
    row = {
        "id": OUTBOX_ID,
        "name": "Session outbox",
        "type": "cloud_outbox",
        "config": {},
        "managed_key": f"cloud-outbox:{user}",
        "read_only": None,
        "is_global": False,
        "created_by": user,
        "project_read_only": None,
    }
    row.update(changes)
    return row


class _Ctx:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_exc):
        return False


def _store(rows, project=PROJECT, role="owner"):
    conn = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    store = MagicMock()
    store.acquire = MagicMock(return_value=_Ctx(conn))
    store.get_project = AsyncMock(return_value=project)
    store.get_user_project_roles = AsyncMock(
        return_value={PROJECT_ID: role} if role else {}
    )
    return store, conn


async def _admit(
    rows, *, project=PROJECT, outbox_allowed=True, ids=(CHECKOUT_ID,), role="owner"
):
    store, _conn = _store(rows, project, role)
    return await checkout.admission_stamp(
        store,
        datasource_ids=list(ids),
        project_id=PROJECT_ID if project is not None else None,
        user_id=USER_ID,
        workspace_backend="sandbox",
        outbox_allowed=outbox_allowed,
    )


# =============================================================================
# Admission
# =============================================================================


class TestAdmission:
    @pytest.mark.asyncio
    async def test_an_attached_checkout_is_stamped_with_its_level_and_holds_dispatch(
        self,
    ):
        stamp = await _admit([_checkout_row()])
        assert stamp == {
            "cloud_checkout": {
                "datasource_id": CHECKOUT_ID,
                "access": "reviewed_write_back",
            },
            "cloud_baseline": {"state": "seeding"},
            "cloud_connectors": [
                {
                    "kind": "cloud_folder_checkout",
                    "path": "projects/hand_book/",
                    "folder": "Hand Book",
                    "edits": "reviewed",
                    "access": "reviewed_write_back",
                    "datasource_id": CHECKOUT_ID,
                }
            ],
        }

    @pytest.mark.asyncio
    async def test_a_link_default_is_the_level(self):
        stamp = await _admit([_checkout_row(project_access="read_only")])
        assert stamp["cloud_checkout"]["access"] == "read_only"

    @pytest.mark.asyncio
    async def test_a_viewers_job_never_writes_back(self):
        # Accepting their own job's diff would write a folder they only view.
        stamp = await _admit([_checkout_row()], role="viewer")
        assert stamp["cloud_checkout"]["access"] == "read_only"
        assert stamp["cloud_connectors"][0]["edits"] == "discarded"
        # A non-member (an administrator) keeps every level.
        stamp = await _admit([_checkout_row()], role=None)
        assert stamp["cloud_checkout"]["access"] == "reviewed_write_back"

    @pytest.mark.asyncio
    async def test_an_unreadable_role_costs_the_copy(self):
        store, _conn = _store([_checkout_row()])
        store.get_user_project_roles.side_effect = RuntimeError("db down")
        assert (
            await checkout.admission_stamp(
                store,
                datasource_ids=[CHECKOUT_ID],
                project_id=PROJECT_ID,
                user_id=USER_ID,
                workspace_backend=None,
                outbox_allowed=True,
            )
            == {}
        )

    @pytest.mark.asyncio
    async def test_a_read_only_link_copies_without_write_back(self):
        stamp = await _admit([_checkout_row(project_read_only=True)])
        assert stamp["cloud_checkout"]["access"] == "read_only"
        assert stamp["cloud_connectors"][0]["edits"] == "discarded"

    @pytest.mark.asyncio
    async def test_the_owners_outbox_is_named_beside_the_copy(self):
        stamp = await _admit([_checkout_row(), _outbox_row()])
        assert [e["kind"] for e in stamp["cloud_connectors"]] == [
            "cloud_folder_checkout",
            "cloud_outbox",
        ]
        assert connector_entry(stamp, "cloud_outbox") == {
            "kind": "cloud_outbox",
            "datasource_id": OUTBOX_ID,
        }

    @pytest.mark.asyncio
    async def test_unattended_work_gets_no_outbox(self):
        stamp = await _admit([_outbox_row()], outbox_allowed=False)
        assert stamp == {}

    @pytest.mark.asyncio
    async def test_another_users_outbox_delivers_nothing(self):
        assert await _admit([_outbox_row(user=OTHER_USER)]) == {}

    @pytest.mark.asyncio
    async def test_a_loose_job_still_gets_its_outbox(self):
        stamp = await _admit([_outbox_row()], project=None)
        assert stamp == {
            "cloud_connectors": [{"kind": "cloud_outbox", "datasource_id": OUTBOX_ID}]
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "row",
        [
            _checkout_row(managed_key=f"project-cloud-checkout:{OTHER_PROJECT}"),
            _checkout_row(config={"project_id": OTHER_PROJECT}),
            _checkout_row(managed_key=None),
        ],
    )
    async def test_only_the_projects_own_checkout_copies_anything(self, row):
        assert await _admit([row]) == {}

    @pytest.mark.asyncio
    async def test_a_project_without_a_folder_gets_no_copy(self):
        project = {**PROJECT, "main_cloud_folder_handle": None}
        assert await _admit([_checkout_row()], project=project) == {}

    @pytest.mark.asyncio
    async def test_an_explicit_empty_selection_reads_nothing(self):
        store, conn = _store([_checkout_row()])
        stamp = await checkout.admission_stamp(
            store,
            datasource_ids=[],
            project_id=PROJECT_ID,
            user_id=USER_ID,
            workspace_backend=None,
            outbox_allowed=True,
        )
        assert stamp == {}
        conn.fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_read_failure_costs_the_copy_not_the_admission(self):
        store, conn = _store([])
        conn.fetch.side_effect = RuntimeError("db down")
        stamp = await checkout.admission_stamp(
            store,
            datasource_ids=[CHECKOUT_ID],
            project_id=PROJECT_ID,
            user_id=USER_ID,
            workspace_backend=None,
            outbox_allowed=True,
        )
        assert stamp == {}


class TestProvisioning:
    @pytest.mark.asyncio
    async def test_a_loop_is_told_its_copy_is_applied(self):
        stamp = await checkout.provisioning_stamp(
            MagicMock(),
            job_row={"id": JOB_ID, "context": {"loop_id": "l1"}},
            project=PROJECT,
            loop_floor=True,
        )
        assert stamp["cloud_connectors"][0]["edits"] == "applied"
        assert "cloud_checkout" not in stamp

    @pytest.mark.asyncio
    async def test_an_officer_executor_without_a_checkout_keeps_review(self):
        stamp = await checkout.provisioning_stamp(
            MagicMock(),
            job_row={"id": JOB_ID, "context": {}},
            project=PROJECT,
            loop_floor=True,
        )
        assert stamp["cloud_connectors"][0]["edits"] == "reviewed"

    @pytest.mark.asyncio
    async def test_a_told_loop_is_not_told_again(self):
        stamp = await checkout.provisioning_stamp(
            MagicMock(),
            job_row={"id": JOB_ID, "context": {"cloud_connectors": []}},
            project=PROJECT,
            loop_floor=True,
        )
        assert stamp is None

    @pytest.mark.asyncio
    async def test_admitted_and_subjob_rows_are_left_alone(self):
        store, conn = _store([_checkout_row()])
        for row in (
            {"id": JOB_ID, "context": {"cloud_checkout": {"access": "read_only"}}},
            {"id": JOB_ID, "parent_job_id": "p", "context": {}},
        ):
            assert (
                await checkout.provisioning_stamp(
                    store, job_row=row, project=PROJECT, loop_floor=False
                )
                is None
            )
        conn.fetch.assert_not_awaited()


# =============================================================================
# The task brief
# =============================================================================


class TestTheBrief:
    def _block(self, edits, access=None, baseline=None, extra=()):
        entries = [
            checkout_entry(
                project_slug="hand_book", folder="Hand Book", edits=edits, access=access
            ),
            *extra,
        ]
        return format_cloud_connectors_block(entries, baseline)

    def test_nothing_attached_says_nothing(self):
        assert format_cloud_connectors_block(None) == ""
        assert format_cloud_connectors_block([]) == ""
        assert format_cloud_connectors_block([{"kind": "unknown"}]) == ""

    def test_a_read_only_copy_says_edits_go_nowhere(self):
        block = self._block("discarded", "read_only")
        assert "## Project Cloud Folder" in block
        assert (
            '`projects/hand_book/` in this workspace is a copy of the cloud folder "Hand Book"'
            in block
        )
        assert "SRW never applies the copy's changes" in block
        assert "Only paths under that directory map" not in block

    def test_a_reviewed_copy_says_only_an_accepted_diff_lands(self):
        block = self._block("reviewed", "reviewed_write_back")
        assert "shown to the job's owner as a diff" in block
        assert "Only paths under that directory map to the cloud folder" in block

    def test_a_loops_copy_says_a_conflict_free_diff_is_applied(self):
        assert "applies the changes you commit" in self._block("applied")

    def test_the_seeds_record_is_the_truth(self):
        block = self._block(
            "discarded",
            baseline={
                "state": "ready",
                "project_slug": "renamed",
                "seeded": 3,
                "skipped_binary": 2,
                "failed": 1,
            },
        )
        assert "`projects/renamed/`" in block
        assert "3 file(s) were copied." in block
        assert "2 binary file(s) of the cloud folder were not copied" in block
        assert "1 file(s) could not be copied" in block
        empty = self._block(
            "discarded",
            baseline={"state": "ready", "seeded": 0, "skipped_binary": 0, "failed": 0},
        )
        assert "the cloud folder was empty" in empty
        failed = self._block(
            "discarded", baseline=json.dumps({"state": "failed", "error": "boom"})
        )
        assert "could not be completed (boom)" in failed

    def test_the_outbox_names_what_reaches_the_shared_folder(self):
        block = format_cloud_connectors_block({"cloud_connectors": [outbox_entry()]})
        assert block.startswith("\n\n## Cloud Outbox")
        assert "declared deliverables" in block

    def test_malformed_entries_are_dropped(self):
        assert parse_cloud_connectors(
            [
                {"kind": "cloud_folder_checkout", "edits": "reviewed"},
                {"kind": "cloud_folder_checkout", "path": "p/", "edits": "x"},
                "not-an-entry",
                {"kind": "cloud_outbox"},
            ]
        ) == [{"kind": "cloud_outbox"}]
        with pytest.raises(ValueError):
            checkout_entry(project_slug="x", folder="X", edits="maybe")


# =============================================================================
# The seed's record
# =============================================================================


def _seed_doubles(files: dict[str, bytes]):
    backend = MagicMock()
    backend.is_initialized = True
    backend.list_project_folder = AsyncMock(
        return_value=[
            ProjectFolderEntry(path=path, is_dir=False, etag=f"e-{path}", size=len(b))
            for path, b in files.items()
        ]
    )
    backend.get_project_folder_file_bytes = AsyncMock(
        side_effect=lambda _handle, path: files[path]
    )
    router = MagicMock()
    router.for_backend = MagicMock(return_value=backend)
    gitea = MagicMock()
    gitea.create_or_update_file = AsyncMock(return_value=True)
    gitea.change_files = AsyncMock(return_value=True)
    gitea.get_branch_head_sha = AsyncMock(return_value="base")
    db = MagicMock()
    db.get_job = AsyncMock(return_value={"id": JOB_ID})
    db.merge_job_context = AsyncMock(return_value=True)
    db.update_job_cloud_diff = AsyncMock()
    return router, gitea, db


class TestSeedRecord:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "include_binary, counts",
        [
            (False, {"seeded": 1, "skipped_binary": 1, "failed": 0}),
            (True, {"seeded": 2, "skipped_binary": 0, "failed": 0}),
        ],
    )
    async def test_a_finished_walk_records_what_it_copied(self, include_binary, counts):
        router, gitea, db = _seed_doubles(
            {"notes.md": b"hello", "deck.pdf": b"%PDF\xff\xfe"}
        )
        await seed_project_folder_baseline(
            job_id=JOB_ID,
            project=PROJECT,
            repo_name="job-x",
            branch=None,
            postgres_db=db,
            gitea_client=gitea,
            main_cloud_router=router,
            include_binary=include_binary,
        )
        final = db.merge_job_context.await_args.args[1]["cloud_baseline"]
        assert final["state"] == "ready"
        assert {key: final[key] for key in counts} == counts
        assert final["project_slug"] == "hand_book"
        assert gitea.change_files.await_count == (1 if include_binary else 0)

    @pytest.mark.asyncio
    async def test_an_empty_folder_records_zeros(self):
        router, gitea, db = _seed_doubles({})
        await seed_project_folder_baseline(
            job_id=JOB_ID,
            project=PROJECT,
            repo_name="job-x",
            branch=None,
            postgres_db=db,
            gitea_client=gitea,
            main_cloud_router=router,
        )
        final = db.merge_job_context.await_args.args[1]["cloud_baseline"]
        assert (final["seeded"], final["skipped_binary"], final["failed"]) == (0, 0, 0)


# =============================================================================
# Capture
# =============================================================================


def _tree(*paths, sha="1"):
    return [{"type": "blob", "path": p, "sha": sha} for p in paths]


class TestCapture:
    @pytest.mark.parametrize(
        "context, baseline, applies",
        [
            ({"cloud_checkout": {"access": "read_only"}}, "b", False),
            ({"cloud_checkout": {"access": "reviewed_write_back"}}, "b", True),
            ({}, "b", True),  # an older job: implicit reviewed write-back
            ({}, None, False),
            (json.dumps({"cloud_checkout": {"access": "read_only"}}), "b", False),
        ],
    )
    def test_which_jobs_capture(self, context, baseline, applies):
        job = {"context": context, "cloud_diff_baseline_commit": baseline}
        assert checkout.capture_applies(job) is applies

    @pytest.mark.asyncio
    async def test_what_the_capture_found(self):
        db = SimpleNamespace(update_job_cloud_diff=AsyncMock())
        gitea = SimpleNamespace(
            get_branch_head_sha=AsyncMock(return_value="head"),
            list_tree=AsyncMock(),
        )
        job = {
            "id": JOB_ID,
            "repo_name": "job-x",
            "cloud_diff_baseline_commit": "base",
            "context": {"cloud_baseline": {"project_slug": "hand_book"}},
        }

        async def run(**changes):
            return await capture_checkout_diff(
                job={**job, **changes}, postgres_db=db, gitea_client=gitea
            )

        assert await run(cloud_diff_baseline_commit=None) == {
            "state": "no_baseline",
            "files": 0,
        }
        gitea.get_branch_head_sha.return_value = None
        assert (await run())["state"] == "head_unreadable"
        gitea.get_branch_head_sha.return_value = "base"
        assert (await run())["state"] == "no_changes"
        gitea.get_branch_head_sha.return_value = "head"
        gitea.list_tree.side_effect = [None, _tree()]
        assert (await run())["state"] == "head_unreadable"
        # A change under another slug cannot be written back: no diff.
        gitea.list_tree.side_effect = [
            _tree("projects/hand_book/a.md"),
            _tree("projects/hand_book/a.md", "projects/guess/b.md", "output/r.md"),
        ]
        assert (await run())["state"] == "no_changes"
        db.update_job_cloud_diff.assert_not_awaited()
        gitea.list_tree.side_effect = [
            _tree("projects/hand_book/a.md"),
            _tree("projects/hand_book/a.md", sha="2") + _tree("output/r.md"),
        ]
        assert await run() == {"state": "captured", "files": 1}
        db.update_job_cloud_diff.assert_awaited_once_with(JOB_ID, diff_status="pending")


class TestCriticApproval:
    def _transition(self, *, autonomy, diff_status):
        from orchestrator.services.verification_workflow import (
            critic_verdict_transition,
        )

        target = {
            "id": JOB_ID,
            "diff_status": diff_status,
            "resolved_config": {"autonomy": autonomy},
            "context": {
                "verification_rounds": [
                    {"critic_job_id": "c1", "verdict": "approved", "findings": []}
                ]
            },
        }
        return critic_verdict_transition({"id": "c1", "status": "completed"}, target)

    def test_a_pending_diff_waits_for_its_owner_whatever_the_autonomy(self):
        assert (
            self._transition(autonomy="full", diff_status="pending")["new_status"]
            == "pending_review"
        )

    def test_without_one_full_autonomy_completes(self):
        assert (
            self._transition(autonomy="full", diff_status=None)["new_status"]
            == "completed"
        )
        assert (
            self._transition(autonomy="review", diff_status=None)["new_status"]
            == "pending_review"
        )
