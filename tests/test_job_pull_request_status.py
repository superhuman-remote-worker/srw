"""Job-scoped live PR status: access-gated and credential-safe."""

import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main
from shared.runtime.services.forge import ForgeError
from orchestrator.application import workspace as workspace_composition


def _authorized(user: dict, db):
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


def _job_with_pr(job: dict) -> dict:
    return {
        **job,
        "context": json.dumps(
            {
                "pull_request": {
                    "forge": "github",
                    "repo": "Knaeckebrothero/KurortEngine",
                    "number": 1,
                    "url": "https://github.com/Knaeckebrothero/KurortEngine/pull/1",
                    "head": "design/hotel-rheinland-theme",
                    "base": "main",
                }
            }
        ),
    }


def _repository() -> dict:
    return {
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "name": "KurortEngine",
        "type": "repository",
        "connection_url": "https://github.com/Knaeckebrothero/KurortEngine.git",
        "config": {"forge": "github"},
        "credentials": {"token": "server-only-token"},
    }


class TestJobPullRequestStatusEndpoint:
    @pytest.mark.asyncio
    async def test_historical_job_without_record_does_not_guess_a_pr(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import get_job_pull_request_status

        fake_db.get_job = AsyncMock(
            return_value={**job_a, "context": {"cloud_baseline": {}}}
        )
        fake_db.resolve_datasources_for_job = AsyncMock()
        with _authorized(user_a, fake_db), pytest.raises(HTTPException) as exc:
            await get_job_pull_request_status(
                fake_request,
                str(job_a["id"]),
                dependencies=workspace_composition.job_review_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        assert exc.value.status_code == 404
        fake_db.resolve_datasources_for_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_gets_live_status_from_matching_attached_repository(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import get_job_pull_request_status

        job = _job_with_pr(job_a)
        fake_db.get_job = AsyncMock(return_value=job)
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[_repository()])
        live = {
            "number": 1,
            "url": "https://github.com/Knaeckebrothero/KurortEngine/pull/1",
            "state": "open",
            "head": "design/hotel-rheinland-theme",
            "base": "main",
            "draft": False,
        }

        with (
            _authorized(user_a, fake_db),
            patch(
                "shared.runtime.services.forge.get_pull_request_status",
                AsyncMock(return_value=live),
            ) as read_status,
        ):
            result = await get_job_pull_request_status(
                fake_request,
                str(job_a["id"]),
                dependencies=workspace_composition.job_review_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        target, number = read_status.await_args.args
        assert number == 1
        assert target.forge == "github"
        assert target.owner == "Knaeckebrothero"
        assert target.repo == "KurortEngine"
        assert target.token == "server-only-token"
        assert result == {
            "forge": "github",
            "repo": "Knaeckebrothero/KurortEngine",
            **live,
        }
        assert "server-only-token" not in str(result)

    @pytest.mark.asyncio
    async def test_cross_user_is_rejected_before_credentials_or_forge_are_touched(
        self, user_b, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import get_job_pull_request_status

        fake_db.get_job = AsyncMock(return_value=_job_with_pr(job_a))
        fake_db.resolve_datasources_for_job = AsyncMock()
        with (
            _authorized(user_b, fake_db),
            patch(
                "shared.runtime.services.forge.get_pull_request_status", AsyncMock()
            ) as read_status,
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 403
        fake_db.resolve_datasources_for_job.assert_not_awaited()
        read_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mismatched_repository_is_not_queried(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import get_job_pull_request_status

        fake_db.get_job = AsyncMock(return_value=_job_with_pr(job_a))
        repository = _repository()
        repository["connection_url"] = "https://github.com/acme/not-the-delivery.git"
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[repository])

        with (
            _authorized(user_a, fake_db),
            patch(
                "shared.runtime.services.forge.get_pull_request_status", AsyncMock()
            ) as read_status,
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 409
        read_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_remote_failure_returns_a_bounded_error_without_credentials(
        self, user_a, job_a, fake_db, fake_request
    ):
        from orchestrator.routers.job_review import get_job_pull_request_status

        fake_db.get_job = AsyncMock(return_value=_job_with_pr(job_a))
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[_repository()])
        with (
            _authorized(user_a, fake_db),
            patch(
                "shared.runtime.services.forge.get_pull_request_status",
                AsyncMock(side_effect=ForgeError("403 server-only-token")),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=workspace_composition.job_review_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )

        assert exc.value.status_code == 502
        assert "server-only-token" not in str(exc.value.detail)
