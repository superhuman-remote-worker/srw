"""A job's Gitea link names the repository's real owner, not a fixed ``srw``.

Issue: knowledge-base/knowledge/issues/cockpit_gitea_links_hardcode_owner_srw.md.
Every managed repository is created under ``GITEA_ADMIN_USER``; the generator
sets it to ``srw-admin``, but Cockpit composed ``<giteaUrl>/srw/<repo>`` from a
chart value with the owner baked in, so the link 404'd. The orchestrator is the
one process that knows the owner, so the job API now carries the URL.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from orchestrator.routers import job_reads as job_reads_routes
from orchestrator.routers.job_reads import JobReadsDependencies, router
from orchestrator.services import job_queries, job_reads
from orchestrator.services.gitea import repo_web_url

JOB = "33333333-3333-4333-8333-333333333333"
USER = {"id": "11111111-1111-4111-8111-111111111111", "is_approved": True}


@pytest.fixture
def generator_install(monkeypatch):
    """What the public generator renders: admin ``srw-admin``, Gitea at /git."""
    monkeypatch.setenv("GITEA_URL", "https://srw.example.org/git/")
    monkeypatch.setenv("GITEA_ADMIN_USER", "srw-admin")


class TestRepoWebUrl:
    def test_the_owner_follows_a_non_default_gitea_admin_user(self, generator_install):
        assert (
            repo_web_url("job-647db6e7")
            == "https://srw.example.org/git/srw-admin/job-647db6e7"
        )

    def test_the_owner_defaults_to_the_clients_default(self, monkeypatch):
        monkeypatch.setenv("GITEA_URL", "https://git.example.org")
        monkeypatch.delenv("GITEA_ADMIN_USER", raising=False)

        assert repo_web_url("job-1") == "https://git.example.org/srw/job-1"

    @pytest.mark.parametrize("name", [None, "", "   "])
    def test_no_repository_has_no_url(self, generator_install, name):
        assert repo_web_url(name) is None

    def test_no_public_gitea_url_has_no_url(self, monkeypatch):
        monkeypatch.delenv("GITEA_URL", raising=False)

        assert repo_web_url("job-1") is None

    @pytest.mark.parametrize("name", ["../other", "a/b", "x y"])
    def test_a_name_gitea_would_refuse_never_becomes_a_link(
        self, generator_install, name
    ):
        assert repo_web_url(name) is None


# =============================================================================
# The job reads carry it
# =============================================================================


@pytest.fixture
def reads(monkeypatch, generator_install):
    rows: list[dict[str, Any]] = [
        {"id": UUID(JOB), "status": "completed", "repo_name": "job-647db6e7"},
        {"id": UUID(int=7), "status": "completed", "repo_name": None},
    ]

    async def list_jobs(**_kwargs):
        return {"jobs": [dict(row) for row in rows], "total": len(rows)}

    async def read_job(*, job_id, authorized_job, dependencies):
        return dict(authorized_job)

    async def approved(_request, _store):
        return USER

    async def job_access(_request, _store, _job_id):
        return USER, dict(rows[0])

    monkeypatch.setattr(job_queries, "list_jobs", list_jobs)
    monkeypatch.setattr(job_reads, "read_job", read_job)
    monkeypatch.setattr(
        job_reads_routes, "read_vm_idle_states", AsyncMock(return_value={})
    )
    store = SimpleNamespace(
        container_workspace_creation_views=AsyncMock(return_value={})
    )
    app = FastAPI()
    app.state.job_reads_dependencies_factory = lambda: JobReadsDependencies(
        store=store,
        queries=SimpleNamespace(),
        reads=SimpleNamespace(),
        require_approved_user=approved,
        require_job_access=job_access,
    )
    app.include_router(router)
    return app


async def _get(app, path):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://reads.test"
    ) as client:
        return await client.get(path)


class TestJobReadsCarryTheRepoUrl:
    @pytest.mark.asyncio
    async def test_list_rows_link_the_repository_under_its_real_owner(self, reads):
        response = await _get(reads, "/api/jobs")

        assert response.status_code == 200, response.text
        jobs = response.json()["jobs"]
        assert jobs[0]["gitea_url"] == (
            "https://srw.example.org/git/srw-admin/job-647db6e7"
        )
        assert jobs[1]["gitea_url"] is None

    @pytest.mark.asyncio
    async def test_the_detail_links_the_repository_under_its_real_owner(self, reads):
        response = await _get(reads, f"/api/jobs/{JOB}")

        assert response.status_code == 200, response.text
        assert response.json()["gitea_url"] == (
            "https://srw.example.org/git/srw-admin/job-647db6e7"
        )
