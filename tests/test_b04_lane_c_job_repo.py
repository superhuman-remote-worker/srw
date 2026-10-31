"""R1.B04 lane C — the job workspace-browser (Gitea proxy) routes.

Characterization first: before this extraction only two of the five routes had
any coverage at all (``tests/test_job_access.py`` pins the cross-user 403 on
``list_repo_contents``/``get_repo_file``; ``tests/test_gitea_path_safety.py``
drives three of them for traversal). Nothing pinned the refusal *order*
(gate → 503 → repo resolution), the exact 404 strings, the job-branch default
ref, the ``sha == "main"`` substitution, or the job-prefix tag filter. Those
are written down here before the move is relied on.
"""

from __future__ import annotations

import base64
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers.job_repo import (
    JobRepoDependencies,
    get_repo_diff,
    get_repo_file,
    list_repo_commits,
    list_repo_contents,
    list_repo_tags,
    router as job_repo_router,
)
from orchestrator.services import gitea as gitea_mod, job_repo_reads
from tests._mounted_router import mount_router
from tests._route_inventory import mounted_route_objects


JOB_ID = "a6fa6f2a-9101-4c1e-9b9e-0000000000c1"
USER = {"id": "00000000-0000-0000-0000-0000000000u1".replace("u", "e")}
REPO = "job-a6fa6f2a"
BRANCH = "job/a6fa6f2a"


def _forge(**over):
    forge = SimpleNamespace(
        is_initialized=True,
        list_contents=AsyncMock(return_value=[{"name": "a.py", "type": "file"}]),
        get_file_bytes=AsyncMock(return_value=b"hello"),
        get_commits_between=AsyncMock(return_value={"total_commits": 1, "commits": []}),
        get_commits=AsyncMock(return_value=[{"sha": "abc"}]),
        get_diff=AsyncMock(return_value="--- a\n+++ b\n"),
        get_tags=AsyncMock(
            return_value=[
                {"name": "a6fa6f2a-v1", "sha": "1"},
                {"name": "deadbeef-v1", "sha": "2"},
            ]
        ),
    )
    for key, value in over.items():
        setattr(forge, key, value)
    return forge


def _deps(*, forge=None, resolve=None, gate=None, store=None):
    resolved = resolve or AsyncMock(return_value=(REPO, BRANCH))
    reads = job_repo_reads.JobRepoReadDependencies(
        forge=forge or _forge(), resolve_job_repo=resolved
    )
    route_deps = JobRepoDependencies(
        store=store or SimpleNamespace(name="store"),
        repo_reads=reads,
    )
    if gate is not None:
        route_deps = replace(route_deps, require_job_access=gate)
    else:
        route_deps = replace(
            route_deps,
            require_job_access=AsyncMock(return_value=(USER, {"id": JOB_ID})),
        )
    return route_deps


def _request():
    return SimpleNamespace(headers={})


class TestAuthorizationOrder:
    @pytest.mark.parametrize(
        ("handler", "kwargs"),
        [
            (list_repo_contents, {"path": "", "ref": None}),
            (get_repo_file, {"path": "a.py", "ref": None}),
            (
                list_repo_commits,
                {"sha": "main", "since_ref": None, "page": 1, "limit": 20},
            ),
            (get_repo_diff, {"base": "aaa", "head": "HEAD"}),
            (list_repo_tags, {"all_jobs": False}),
        ],
    )
    @pytest.mark.asyncio
    async def test_the_gate_refuses_before_the_forge_is_ever_touched(
        self, handler, kwargs
    ):
        gate = AsyncMock(side_effect=HTTPException(403, "Access denied"))
        forge = _forge()
        deps = _deps(forge=forge, gate=gate)

        with pytest.raises(HTTPException) as exc:
            await handler(_request(), JOB_ID, dependencies=deps, **kwargs)

        assert exc.value.status_code == 403
        gate.assert_awaited_once_with(_ANY_REQUEST, deps.store, JOB_ID)
        forge.list_contents.assert_not_awaited()
        deps.repo_reads.resolve_job_repo.assert_not_awaited()

    @pytest.mark.parametrize(
        ("handler", "kwargs", "detail"),
        [
            (list_repo_contents, {"path": "", "ref": None}, "Gitea not available"),
            (get_repo_file, {"path": "a.py", "ref": None}, "Gitea not available"),
            (
                list_repo_commits,
                {"sha": "main", "since_ref": None, "page": 1, "limit": 20},
                "Gitea not available",
            ),
            (get_repo_diff, {"base": "aaa", "head": "HEAD"}, "Gitea not available"),
            (list_repo_tags, {"all_jobs": False}, "Gitea not available"),
        ],
    )
    @pytest.mark.asyncio
    async def test_an_uninitialized_forge_is_a_503_before_repo_resolution(
        self, handler, kwargs, detail
    ):
        deps = _deps(forge=_forge(is_initialized=False))

        with pytest.raises(HTTPException) as exc:
            await handler(_request(), JOB_ID, dependencies=deps, **kwargs)

        assert exc.value.status_code == 503
        assert exc.value.detail == detail
        deps.repo_reads.resolve_job_repo.assert_not_awaited()


class _AnyRequest:
    def __eq__(self, other):
        return True

    def __repr__(self):
        return "<any request>"


_ANY_REQUEST = _AnyRequest()


class TestListRepoContents:
    @pytest.mark.asyncio
    async def test_the_job_branch_is_the_default_ref(self):
        deps = _deps()

        await list_repo_contents(
            _request(), JOB_ID, path="src", ref=None, dependencies=deps
        )

        deps.repo_reads.forge.list_contents.assert_awaited_once_with(
            REPO, "src", ref=BRANCH
        )

    @pytest.mark.asyncio
    async def test_an_explicit_ref_wins_over_the_job_branch(self):
        deps = _deps()

        await list_repo_contents(
            _request(), JOB_ID, path="src", ref="v2", dependencies=deps
        )

        deps.repo_reads.forge.list_contents.assert_awaited_once_with(
            REPO, "src", ref="v2"
        )

    @pytest.mark.asyncio
    async def test_a_missing_path_reports_the_root_as_a_slash(self):
        deps = _deps(forge=_forge(list_contents=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await list_repo_contents(
                _request(), JOB_ID, path="", ref=None, dependencies=deps
            )

        assert exc.value.status_code == 404
        assert exc.value.detail == f"Path '/' not found in repo for job '{JOB_ID}'"

    @pytest.mark.asyncio
    async def test_an_empty_listing_is_returned_not_refused(self):
        deps = _deps(forge=_forge(list_contents=AsyncMock(return_value=[])))

        assert (
            await list_repo_contents(
                _request(), JOB_ID, path="src", ref=None, dependencies=deps
            )
            == []
        )


class TestGetRepoFile:
    @pytest.mark.asyncio
    async def test_returns_path_content_and_size(self):
        deps = _deps()

        assert await get_repo_file(
            _request(), JOB_ID, path="a.py", ref=None, dependencies=deps
        ) == {"path": "a.py", "content": "hello", "encoding": "utf-8", "size": 5}
        deps.repo_reads.forge.get_file_bytes.assert_awaited_once_with(
            REPO, "a.py", ref=BRANCH
        )

    @pytest.mark.asyncio
    async def test_a_binary_file_is_served_base64_not_a_404(self):
        # A PDF deliverable: the header is ASCII but the body bytes are not
        # UTF-8, which used to read as "not found".
        pdf = b"%PDF-1.7\n\xe2\xe3\xcf\xd3\n\x00\xff\x89"
        deps = _deps(forge=_forge(get_file_bytes=AsyncMock(return_value=pdf)))

        result = await get_repo_file(
            _request(), JOB_ID, path="output/report.pdf", ref=None, dependencies=deps
        )

        assert result == {
            "path": "output/report.pdf",
            "content": base64.b64encode(pdf).decode("ascii"),
            "encoding": "base64",
            "size": len(pdf),
        }
        assert base64.b64decode(result["content"]) == pdf

    @pytest.mark.asyncio
    async def test_a_binary_file_through_the_real_gitea_client_is_not_a_404(self):
        # The original bug lived in the client: its UTF-8 decode raised, the
        # broad except returned None, and the route said "not found".
        blob = b"SQLite format 3\x00\x10\x00\x01\x01\x00@  \x00\x00\x00\x02\xff\xfe"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"type": "file", "content": base64.b64encode(blob).decode()},
            )

        client = gitea_mod.GiteaClient.__new__(gitea_mod.GiteaClient)
        client._initialized = True
        client._url = "http://gitea"
        client._user = "srw"
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        deps = _deps(forge=client)

        result = await get_repo_file(
            _request(), JOB_ID, path="output/shop.db", ref=None, dependencies=deps
        )

        assert result["encoding"] == "base64"
        assert result["size"] == len(blob)
        assert base64.b64decode(result["content"]) == blob

    @pytest.mark.asyncio
    async def test_utf8_text_keeps_its_character_length_as_size(self):
        text = "Größe: 3 €"
        deps = _deps(
            forge=_forge(get_file_bytes=AsyncMock(return_value=text.encode("utf-8")))
        )

        assert await get_repo_file(
            _request(), JOB_ID, path="notes.md", ref=None, dependencies=deps
        ) == {"path": "notes.md", "content": text, "encoding": "utf-8", "size": 10}

    @pytest.mark.asyncio
    async def test_a_missing_file_is_a_404_naming_the_path(self):
        deps = _deps(forge=_forge(get_file_bytes=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await get_repo_file(
                _request(), JOB_ID, path="gone.py", ref=None, dependencies=deps
            )

        assert exc.value.status_code == 404
        assert (
            exc.value.detail == f"File 'gone.py' not found in repo for job '{JOB_ID}'"
        )

    @pytest.mark.asyncio
    async def test_an_empty_file_is_served_not_treated_as_missing(self):
        deps = _deps(forge=_forge(get_file_bytes=AsyncMock(return_value=b"")))

        assert await get_repo_file(
            _request(), JOB_ID, path="empty", ref=None, dependencies=deps
        ) == {"path": "empty", "content": "", "encoding": "utf-8", "size": 0}


class TestListRepoCommits:
    @pytest.mark.asyncio
    async def test_the_literal_main_default_is_replaced_by_the_job_branch(self):
        deps = _deps()

        await list_repo_commits(
            _request(),
            JOB_ID,
            sha="main",
            since_ref=None,
            page=1,
            limit=20,
            dependencies=deps,
        )

        deps.repo_reads.forge.get_commits.assert_awaited_once_with(
            REPO, sha=BRANCH, page=1, limit=20
        )

    @pytest.mark.asyncio
    async def test_main_survives_when_the_job_has_no_branch(self):
        deps = _deps(resolve=AsyncMock(return_value=(REPO, None)))

        await list_repo_commits(
            _request(),
            JOB_ID,
            sha="main",
            since_ref=None,
            page=1,
            limit=20,
            dependencies=deps,
        )

        deps.repo_reads.forge.get_commits.assert_awaited_once_with(
            REPO, sha="main", page=1, limit=20
        )

    @pytest.mark.asyncio
    async def test_since_ref_goes_through_the_compare_path_and_is_returned_raw(self):
        deps = _deps()

        result = await list_repo_commits(
            _request(),
            JOB_ID,
            sha="main",
            since_ref="base-sha",
            page=1,
            limit=20,
            dependencies=deps,
        )

        deps.repo_reads.forge.get_commits_between.assert_awaited_once_with(
            REPO, "base-sha", BRANCH
        )
        deps.repo_reads.forge.get_commits.assert_not_awaited()
        assert result == {"total_commits": 1, "commits": []}

    @pytest.mark.asyncio
    async def test_a_failed_compare_names_both_refs(self):
        deps = _deps(forge=_forge(get_commits_between=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await list_repo_commits(
                _request(),
                JOB_ID,
                sha="main",
                since_ref="base-sha",
                page=1,
                limit=20,
                dependencies=deps,
            )

        assert exc.value.status_code == 404
        assert exc.value.detail == (
            f"Could not compare base-sha...{BRANCH} in repo for job '{JOB_ID}'"
        )

    @pytest.mark.asyncio
    async def test_a_listing_is_wrapped_with_its_length(self):
        deps = _deps()

        assert await list_repo_commits(
            _request(),
            JOB_ID,
            sha="abc",
            since_ref=None,
            page=2,
            limit=5,
            dependencies=deps,
        ) == {"total_commits": 1, "commits": [{"sha": "abc"}]}

    @pytest.mark.asyncio
    async def test_no_commits_is_a_404(self):
        deps = _deps(forge=_forge(get_commits=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await list_repo_commits(
                _request(),
                JOB_ID,
                sha="abc",
                since_ref=None,
                page=1,
                limit=20,
                dependencies=deps,
            )

        assert exc.value.status_code == 404
        assert exc.value.detail == f"No commits found in repo for job '{JOB_ID}'"


class TestGetRepoDiff:
    @pytest.mark.asyncio
    async def test_the_job_branch_is_not_substituted_into_a_diff(self):
        deps = _deps()

        result = await get_repo_diff(
            _request(), JOB_ID, base="aaa", head="HEAD", dependencies=deps
        )

        deps.repo_reads.forge.get_diff.assert_awaited_once_with(REPO, "aaa", "HEAD")
        assert result == {"base": "aaa", "head": "HEAD", "diff": "--- a\n+++ b\n"}

    @pytest.mark.asyncio
    async def test_a_missing_diff_names_both_refs(self):
        deps = _deps(forge=_forge(get_diff=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await get_repo_diff(
                _request(), JOB_ID, base="aaa", head="bbb", dependencies=deps
            )

        assert exc.value.status_code == 404
        assert exc.value.detail == (
            f"Could not get diff aaa...bbb in repo for job '{JOB_ID}'"
        )


class TestListRepoTags:
    @pytest.mark.asyncio
    async def test_tags_are_filtered_to_the_jobs_short_id_prefix(self):
        deps = _deps()

        assert await list_repo_tags(
            _request(), JOB_ID, all_jobs=False, dependencies=deps
        ) == [{"name": "a6fa6f2a-v1", "sha": "1"}]

    @pytest.mark.asyncio
    async def test_all_jobs_returns_the_whole_repo(self):
        deps = _deps()

        assert (
            len(
                await list_repo_tags(
                    _request(), JOB_ID, all_jobs=True, dependencies=deps
                )
            )
            == 2
        )

    @pytest.mark.asyncio
    async def test_no_tags_at_all_is_a_404(self):
        deps = _deps(forge=_forge(get_tags=AsyncMock(return_value=None)))

        with pytest.raises(HTTPException) as exc:
            await list_repo_tags(_request(), JOB_ID, all_jobs=False, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == f"No tags found in repo for job '{JOB_ID}'"

    @pytest.mark.asyncio
    async def test_an_empty_tag_list_is_not_a_404(self):
        deps = _deps(forge=_forge(get_tags=AsyncMock(return_value=[])))

        assert (
            await list_repo_tags(_request(), JOB_ID, all_jobs=False, dependencies=deps)
            == []
        )


# =============================================================================
# Wire: paths, query parameters and per-invocation dependency resolution
# =============================================================================


class TestJobRepoWire:
    def test_routes_keep_their_paths_and_methods(self):
        app = mount_router(job_repo_router)
        # ``app.routes`` is not the API on FastAPI >= 0.139: an include is one
        # ``_IncludedRouter`` wrapper with no path or methods of its own, so the
        # obvious comprehension silently sees zero of these routes. The pin is
        # ``fastapi>=0.109.0``, so a fresh install resolves the new shape while a
        # long-lived venv sits on the old one — read it through the helper.
        declared = {
            (route.path, tuple(sorted(route.methods)))
            for route in mounted_route_objects(app)
        }

        assert ("/api/jobs/{job_id}/repo/contents", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/repo/file", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/repo/commits", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/repo/diff", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/repo/tags", ("GET",)) in declared

    def test_required_query_parameters_are_still_required(self):
        deps = _deps()
        app = mount_router(
            job_repo_router,
            factories={"job_repo_dependencies_factory": lambda: deps},
        )

        with TestClient(app) as client:
            assert client.get(f"/api/jobs/{JOB_ID}/repo/file").status_code == 422
            assert client.get(f"/api/jobs/{JOB_ID}/repo/diff").status_code == 422
            # contents/commits/tags all have defaults for every parameter.
            assert client.get(f"/api/jobs/{JOB_ID}/repo/contents").status_code == 200
            assert client.get(f"/api/jobs/{JOB_ID}/repo/commits").status_code == 200
            assert client.get(f"/api/jobs/{JOB_ID}/repo/tags").status_code == 200

    def test_the_commit_limit_bounds_are_enforced_by_the_route(self):
        deps = _deps()
        app = mount_router(
            job_repo_router,
            factories={"job_repo_dependencies_factory": lambda: deps},
        )

        with TestClient(app) as client:
            assert (
                client.get(
                    f"/api/jobs/{JOB_ID}/repo/commits", params={"limit": 101}
                ).status_code
                == 422
            )
            assert (
                client.get(
                    f"/api/jobs/{JOB_ID}/repo/commits", params={"page": 0}
                ).status_code
                == 422
            )

    def test_a_rebound_forge_is_observed_by_the_next_dependency_build(self):
        first = _deps()
        second = _deps(forge=_forge(is_initialized=False))
        holder = {"deps": first}
        app = mount_router(
            job_repo_router,
            factories={"job_repo_dependencies_factory": lambda: holder["deps"]},
        )

        with TestClient(app) as client:
            assert client.get(f"/api/jobs/{JOB_ID}/repo/tags").status_code == 200
            holder["deps"] = second
            response = client.get(f"/api/jobs/{JOB_ID}/repo/tags")

        assert response.status_code == 503
        assert response.json() == {"detail": "Gitea not available"}
