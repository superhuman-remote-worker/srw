"""Renaming a job changes its display title and never its task text.

The service rules and the mounted route are exercised with a controlled store;
the SQL (the write, the three reads that carry the title, and search) runs in
test_job_rename_real_postgres.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import asyncpg
import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers.job_lifecycle import (
    JobControlRouteDependencies,
    router as job_lifecycle_router,
)
from orchestrator.services.job_rename import (
    JOB_TITLE_MAX_LENGTH,
    normalize_job_title,
    rename_job,
)
from tests._mounted_router import mount_router

JOB = "33333333-3333-4333-8333-333333333333"


class TestNormalizeJobTitle:
    def test_a_title_is_one_trimmed_line(self):
        assert normalize_job_title("  Quarterly\n report \t review ") == (
            "Quarterly report review"
        )

    @pytest.mark.parametrize("raw", [None, "", "   ", "\n\t"])
    def test_blank_means_no_title(self, raw):
        assert normalize_job_title(raw) is None


class TestRenameJob:
    @pytest.mark.asyncio
    async def test_stores_the_normalized_title_and_returns_it(self):
        store = AsyncMock()
        store.update_job_title.return_value = True

        result = await rename_job(store, JOB, "  Invoice audit  ")

        store.update_job_title.assert_awaited_once_with(JOB, "Invoice audit")
        assert result == {"job_id": JOB, "title": "Invoice audit"}

    @pytest.mark.asyncio
    async def test_a_blank_title_clears_the_name(self):
        store = AsyncMock()
        store.update_job_title.return_value = True

        result = await rename_job(store, JOB, "   ")

        store.update_job_title.assert_awaited_once_with(JOB, None)
        assert result == {"job_id": JOB, "title": None}

    @pytest.mark.asyncio
    async def test_the_length_limit_counts_the_normalized_title(self):
        store = AsyncMock()
        store.update_job_title.return_value = True
        padded = "  " + "x" * JOB_TITLE_MAX_LENGTH + "  "

        await rename_job(store, JOB, padded)

        store.update_job_title.assert_awaited_once_with(JOB, "x" * JOB_TITLE_MAX_LENGTH)

    @pytest.mark.asyncio
    async def test_a_title_over_the_limit_is_refused_before_any_write(self):
        store = AsyncMock()

        with pytest.raises(HTTPException) as refused:
            await rename_job(store, JOB, "x" * (JOB_TITLE_MAX_LENGTH + 1))

        assert refused.value.status_code == 400
        store.update_job_title.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_job_that_is_gone_is_a_404(self):
        store = AsyncMock()
        store.update_job_title.return_value = False

        with pytest.raises(HTTPException) as missing:
            await rename_job(store, JOB, "Name")

        assert missing.value.status_code == 404

    @pytest.mark.asyncio
    async def test_a_row_guard_refusal_is_a_retryable_conflict(self):
        """Some row guards refuse every write to a job for a short time."""
        store = AsyncMock()
        store.update_job_title.side_effect = asyncpg.CheckViolationError(
            "Pinned Job idle operation owns dispatch"
        )

        with pytest.raises(HTTPException) as conflict:
            await rename_job(store, JOB, "Name")

        assert conflict.value.status_code == 409


def _client(store, require_job_access) -> httpx.AsyncClient:
    dependencies = JobControlRouteDependencies(
        operations=None,
        store=store,
        require_job_access=require_job_access,
        require_internal_or_job_access=AsyncMock(),
        require_internal=AsyncMock(),
    )
    app = mount_router(
        job_lifecycle_router,
        factories={"job_control_route_dependencies_factory": lambda: dependencies},
    )
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


class TestRenameRoute:
    @pytest.mark.asyncio
    async def test_patch_renames_a_job_the_caller_can_see(self):
        store = AsyncMock()
        store.update_job_title.return_value = True
        access = AsyncMock(return_value=({"id": "u"}, {"id": JOB}))

        async with _client(store, access) as client:
            response = await client.patch(
                f"/api/jobs/{JOB}", json={"title": "Invoice audit"}
            )

        assert response.status_code == 200
        assert response.json() == {"job_id": JOB, "title": "Invoice audit"}
        assert access.await_args.args[1:] == (store, JOB)
        store.update_job_title.assert_awaited_once_with(JOB, "Invoice audit")

    @pytest.mark.asyncio
    async def test_null_clears_the_title(self):
        store = AsyncMock()
        store.update_job_title.return_value = True
        access = AsyncMock(return_value=({"id": "u"}, {"id": JOB}))

        async with _client(store, access) as client:
            response = await client.patch(f"/api/jobs/{JOB}", json={"title": None})

        assert response.status_code == 200
        assert response.json() == {"job_id": JOB, "title": None}
        store.update_job_title.assert_awaited_once_with(JOB, None)

    @pytest.mark.asyncio
    async def test_a_body_without_title_is_rejected(self):
        """An empty PATCH must not read as "clear the title"."""
        store = AsyncMock()
        access = AsyncMock(return_value=({"id": "u"}, {"id": JOB}))

        async with _client(store, access) as client:
            response = await client.patch(f"/api/jobs/{JOB}", json={})

        assert response.status_code == 422
        store.update_job_title.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_caller_without_access_writes_nothing(self):
        store = AsyncMock()
        access = AsyncMock(
            side_effect=HTTPException(
                status_code=403, detail="Not authorized to access this job"
            )
        )

        async with _client(store, access) as client:
            response = await client.patch(f"/api/jobs/{JOB}", json={"title": "Mine"})

        assert response.status_code == 403
        store.update_job_title.assert_not_awaited()
