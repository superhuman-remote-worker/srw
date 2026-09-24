"""Caller-shaped paths and names are refused at the Gitea sink, never sent.

Security audit 2026-08-27, findings #3/#4: ``GiteaClient`` authenticates as
the instance administrator and used to splice a caller-controlled path
straight into ``/repos/{owner}/{repo}/contents/{path}``. httpx normalises dot
segments before sending, so ``../../<owner>/<repo>/contents/x`` from a user's
own job id read any repository in the instance -- via the cockpit proxy
routes and the MCP ``get_job_file`` tool alike. These tests pin the fix at
the sink: a bad path, ref, or name raises ``GiteaPathError`` before any
request leaves the process, and a good path leaves exactly percent-encoded.

The client runs over a real ``httpx.MockTransport`` so the assertion is on
the bytes that would hit the wire, not on a string handed to a mock.
"""

import dataclasses
import importlib
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.application import http as http_composition  # noqa: E402
from orchestrator.application import workspace as workspace_composition  # noqa: E402
from orchestrator.routers import job_repo as job_repo_routes  # noqa: E402
from orchestrator.services import gitea as gitea_mod  # noqa: E402
from orchestrator.services import (  # noqa: E402
    subjob_output as subjob_output_operations,
)
from orchestrator.services.gitea import (  # noqa: E402
    GiteaPathError,
    encode_compare_ref,
    encode_repo_path,
    encode_repo_ref,
    validate_gitea_name,
)

BASE = "http://gitea"
REPO = "job-1"
FILE_BODY = {"content": "aGVsbG8=", "sha": "abc", "type": "file"}  # "hello"

# Every shape from the audit brief plus the encoded/dotted variants that
# reach the same place after one decode or one normalisation pass.
TRAVERSAL_PATHS = [
    "../../other/repo/contents/README.md",
    "..%2F..%2Fother",
    "%2e%2e/%2e%2e/other",
    "foo/../../bar",
    "/etc/passwd",
    "a//b",
    "a\\..\\b",
    "output/%00/report.md",
    "output/x\x00y.md",
    "./output/report.md",
    "output/./report.md",
]

BAD_NAMES = ["../x", "..", ".", "", "a/b", "a b", "a..b", "x\x00", "ü", "a%2Fb"]
GOOD_NAMES = ["job-12345678", "a.b-c_d", "Session_1", "srw", ".dotfirst"]


def _client() -> tuple[gitea_mod.GiteaClient, list[httpx.Request]]:
    """A GiteaClient whose httpx client records every request it would send."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=FILE_BODY)

    gc = gitea_mod.GiteaClient.__new__(gitea_mod.GiteaClient)
    gc._initialized = True
    gc._url = BASE
    gc._user = "srw"
    gc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return gc, seen


def _wire_path(request: httpx.Request) -> bytes:
    """The percent-encoded path exactly as it would leave the process."""
    return request.url.raw_path.split(b"?", 1)[0]


# ---------------------------------------------------------------------------
# Helpers in isolation
# ---------------------------------------------------------------------------


class TestEncodeRepoPath:
    @pytest.mark.parametrize("path", TRAVERSAL_PATHS)
    def test_rejects_every_escape_shape(self, path):
        with pytest.raises(GiteaPathError):
            encode_repo_path(path)

    @pytest.mark.parametrize(
        ("path", "encoded"),
        [
            ("docs/sub dir/файл.md", "docs/sub%20dir/%D1%84%D0%B0%D0%B9%D0%BB.md"),
            ("src/a.b-c_d/e.py", "src/a.b-c_d/e.py"),
            ("docs/", "docs"),
            ("100%.md", "100%25.md"),
            ("a#b?c&d.md", "a%23b%3Fc%26d.md"),
            ("output/job_frozen.json", "output/job_frozen.json"),
        ],
    )
    def test_encodes_each_segment_and_keeps_separators(self, path, encoded):
        assert encode_repo_path(path) == encoded

    def test_root_only_when_explicitly_allowed(self):
        assert encode_repo_path("", allow_empty=True) == ""
        assert encode_repo_path("/", allow_empty=True) == ""
        with pytest.raises(GiteaPathError):
            encode_repo_path("")

    def test_second_trailing_slash_is_still_an_empty_segment(self):
        with pytest.raises(GiteaPathError):
            encode_repo_path("docs//")

    def test_non_string_is_refused(self):
        with pytest.raises(GiteaPathError):
            encode_repo_path(None)  # type: ignore[arg-type]


class TestEncodeRepoRef:
    def test_slashed_branch_travels_as_one_segment(self):
        assert encode_repo_ref("job/abc") == "job%2Fabc"
        assert encode_repo_ref("main") == "main"
        assert encode_repo_ref("a" * 40) == "a" * 40

    @pytest.mark.parametrize("ref", ["../../x", "..%2F..%2Fx", "/main", "a//b", ""])
    def test_rejects_escape_shapes(self, ref):
        with pytest.raises(GiteaPathError):
            encode_repo_ref(ref)


class TestValidateGiteaName:
    @pytest.mark.parametrize("name", BAD_NAMES)
    def test_rejects(self, name):
        with pytest.raises(GiteaPathError):
            validate_gitea_name(name)

    @pytest.mark.parametrize("name", GOOD_NAMES)
    def test_accepts_gitea_charset(self, name):
        assert validate_gitea_name(name) == name

    def test_error_is_a_value_error_with_the_kind(self):
        with pytest.raises(ValueError, match="owner"):
            validate_gitea_name("../x", kind="owner")


# ---------------------------------------------------------------------------
# The sink: nothing leaves the process on a refused value
# ---------------------------------------------------------------------------


class TestReadSinksRefuseTraversal:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", TRAVERSAL_PATHS)
    async def test_file_readers_raise_before_any_request(self, path):
        gc, seen = _client()
        for method in (gc.get_file_bytes, gc.get_file_content, gc.get_file):
            with pytest.raises(GiteaPathError):
                await method(REPO, path)
        with pytest.raises(GiteaPathError):
            await gc.list_contents(REPO, path)
        assert seen == []

    @pytest.mark.asyncio
    async def test_write_sinks_raise_before_any_request(self):
        gc, seen = _client()
        bad = "../../other/repo/contents/README.md"
        with pytest.raises(GiteaPathError):
            await gc.create_or_update_file(REPO, bad, "x", "msg")
        with pytest.raises(GiteaPathError):
            await gc.delete_file(REPO, bad, "msg")
        with pytest.raises(GiteaPathError):
            await gc.delete_path(REPO, "main", bad, "msg")
        assert seen == []

    @pytest.mark.asyncio
    async def test_ref_sinks_raise_before_any_request(self, tmp_path):
        gc, seen = _client()
        bad = "..%2F..%2Fother"
        with pytest.raises(GiteaPathError):
            await gc.get_diff(REPO, "../../x", "main")
        with pytest.raises(GiteaPathError):
            await gc.get_compare(REPO, "main", bad)
        with pytest.raises(GiteaPathError):
            await gc.download_repo_archive(REPO, "../x", str(tmp_path / "a.tgz"))
        with pytest.raises(GiteaPathError):
            await gc.get_branch_head_sha(REPO, bad)
        with pytest.raises(GiteaPathError):
            await gc.list_tree(REPO, "../../x")
        with pytest.raises(GiteaPathError):
            await gc.delete_branch(REPO, "../../x")
        assert seen == []

    @pytest.mark.asyncio
    async def test_repo_name_traversal_is_refused_everywhere(self):
        gc, seen = _client()
        with pytest.raises(GiteaPathError):
            await gc.get_file_bytes("../x", "README.md")
        with pytest.raises(GiteaPathError):
            await gc.list_contents("../x")
        with pytest.raises(GiteaPathError):
            await gc.get_commits("../x")
        with pytest.raises(GiteaPathError):
            await gc.list_branches("..")
        with pytest.raises(GiteaPathError):
            await gc.create_pr("other/repo", "t", "head")
        with pytest.raises(GiteaPathError):
            await gc.add_collaborator(REPO, "../admin")
        with pytest.raises(GiteaPathError):
            gc.clean_repo_url("../x")
        assert seen == []

    @pytest.mark.asyncio
    async def test_owner_namespace_is_validated_too(self):
        gc, seen = _client()
        gc._user = "../other"
        with pytest.raises(GiteaPathError):
            await gc.get_file_bytes(REPO, "README.md")
        assert seen == []

    @pytest.mark.asyncio
    async def test_pull_index_is_coerced_to_an_integer(self):
        gc, seen = _client()
        with pytest.raises(ValueError):
            await gc.merge_pr(REPO, "1/../../x")  # type: ignore[arg-type]
        assert seen == []


# ---------------------------------------------------------------------------
# The sink: a good path leaves exactly percent-encoded
# ---------------------------------------------------------------------------


class TestEncodedUrlOnTheWire:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "wire"),
        [
            (
                "docs/sub dir/файл.md",
                b"/api/v1/repos/srw/job-1/contents/docs/sub%20dir/%D1%84%D0%B0%D0%B9%D0%BB.md",
            ),
            ("src/a.b-c_d/e.py", b"/api/v1/repos/srw/job-1/contents/src/a.b-c_d/e.py"),
        ],
    )
    async def test_file_bytes_request_path(self, path, wire):
        gc, seen = _client()
        assert await gc.get_file_bytes(REPO, path) == b"hello"
        assert len(seen) == 1
        assert _wire_path(seen[0]) == wire
        assert seen[0].url.query == b""

    @pytest.mark.asyncio
    async def test_ref_query_param_is_separate_from_the_path(self):
        gc, seen = _client()
        await gc.get_file_content(REPO, "docs/sub dir/x.md", ref="job/abc")
        assert _wire_path(seen[0]) == (
            b"/api/v1/repos/srw/job-1/contents/docs/sub%20dir/x.md"
        )
        assert seen[0].url.query == b"ref=job%2Fabc"

    @pytest.mark.asyncio
    async def test_list_contents_root_and_directory_spelling(self):
        gc, seen = _client()
        await gc.list_contents(REPO, "")
        await gc.list_contents(REPO, "docs/")
        assert [_wire_path(r) for r in seen] == [
            b"/api/v1/repos/srw/job-1/contents",
            b"/api/v1/repos/srw/job-1/contents/docs",
        ]

    @pytest.mark.asyncio
    async def test_refs_keep_their_route_specific_encoding(self):
        gc, seen = _client()
        await gc.get_branch_head_sha(REPO, "job/abc")
        await gc.list_tree(REPO, "job/abc")
        await gc.get_compare(REPO, "main", "job/abc")
        await gc.get_diff(REPO, "main", "job/abc")
        assert [_wire_path(r) for r in seen] == [
            b"/api/v1/repos/srw/job-1/branches/job%2Fabc",
            b"/api/v1/repos/srw/job-1/git/trees/job%2Fabc",
            b"/api/v1/repos/srw/job-1/compare/main...job/abc",
            b"/api/v1/repos/srw/job-1/compare/main...job/abc.diff",
        ]

    @pytest.mark.asyncio
    async def test_pull_request_routes_are_unchanged(self):
        gc, seen = _client()
        await gc.probe_pr_merged(REPO, 41)
        await gc.comment_on_pr(REPO, 41, "hi")
        assert [_wire_path(r) for r in seen] == [
            b"/api/v1/repos/srw/job-1/pulls/41/merge",
            b"/api/v1/repos/srw/job-1/issues/41/comments",
        ]


# ---------------------------------------------------------------------------
# The proxy routes and the 400 mapping the application installs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def orch_main():
    return importlib.import_module("orchestrator.main")


def _request() -> MagicMock:
    req = MagicMock()
    req.headers = {}
    req.cookies = {}
    return req


class TestProxyRoutes:
    @pytest.mark.asyncio
    async def test_repo_file_and_contents_refuse_traversal_before_gitea(
        self, orch_main
    ):
        gc, seen = _client()
        admin = {"id": "00000000-0000-0000-0000-000000000099", "is_admin": True}
        resources = orch_main.app.state.resources
        with (
            patch.object(resources, "gitea_client", gc),
            patch.object(
                subjob_output_operations,
                "resolve_job_repo",
                AsyncMock(return_value=(REPO, None)),
            ),
        ):
            # ``require_job_access`` is a field default on the route
            # dependencies, so the gate is replaced there rather than patched
            # on a module the router never reads.
            deps = dataclasses.replace(
                workspace_composition.job_repo_dependencies(resources),
                require_job_access=AsyncMock(return_value=(admin, {"id": "job-1"})),
            )
            assert deps.repo_reads.forge is gc
            with pytest.raises(GiteaPathError):
                await job_repo_routes.get_repo_file(
                    _request(),
                    "job-1",
                    path="../../other/repo/contents/README.md",
                    ref=None,
                    dependencies=deps,
                )
            with pytest.raises(GiteaPathError):
                await job_repo_routes.list_repo_contents(
                    _request(),
                    "job-1",
                    path="..%2F..%2Fother",
                    ref=None,
                    dependencies=deps,
                )
            with pytest.raises(GiteaPathError):
                await job_repo_routes.get_repo_diff(
                    _request(), "job-1", base="../../x", head="HEAD", dependencies=deps
                )
        assert seen == []

    @pytest.mark.asyncio
    async def test_gitea_path_error_is_a_400_not_a_500(self, orch_main):
        handler = orch_main.app.exception_handlers[GiteaPathError]
        assert handler is http_composition.gitea_path_error_handler
        response = await handler(_request(), GiteaPathError("nope"))
        assert response.status_code == 400
        assert json.loads(response.body) == {"detail": "nope"}


# ---------------------------------------------------------------------------
# compare/{base}...{head}: the cross-repository separator
# ---------------------------------------------------------------------------


class TestCompareRefSeparator:
    """``owner:branch`` is Gitea's cross-repository compare syntax.

    Its REST handler resolves the owner only through a real fork relation, so
    SRW's sibling job repositories are unreachable that way today — but that
    is upstream's invariant, and ``base``/``head`` arrive straight from the
    caller on ``/api/jobs/{id}/repo/diff``. The separator is refused here so
    the boundary does not depend on it.
    """

    @pytest.mark.parametrize(
        "ref",
        [
            "srw:job-0519ea8f",
            "other-owner:main",
            "main...srw:secret",
            "srw%3Ajob-2",
            ":main",
            "main:",
        ],
    )
    def test_rejects_the_repository_separator(self, ref):
        with pytest.raises(GiteaPathError):
            encode_compare_ref(ref)

    @pytest.mark.parametrize(
        ("ref", "encoded"),
        [
            ("main", "main"),
            ("job/abc", "job/abc"),
            ("a" * 40, "a" * 40),
            ("release/1.2", "release/1.2"),
        ],
    )
    def test_keeps_legitimate_refs_and_their_slashes(self, ref, encoded):
        assert encode_compare_ref(ref) == encoded

    @pytest.mark.parametrize("ref", TRAVERSAL_PATHS)
    def test_still_rejects_every_escape_shape(self, ref):
        with pytest.raises(GiteaPathError):
            encode_compare_ref(ref)

    @pytest.mark.asyncio
    async def test_compare_and_diff_refuse_before_any_request(self):
        gc, seen = _client()
        for call in (gc.get_compare, gc.get_diff):
            with pytest.raises(GiteaPathError):
                await call(REPO, "main", "srw:job-0519ea8f")
            with pytest.raises(GiteaPathError):
                await call(REPO, "srw:job-0519ea8f", "main")
        assert seen == []

    @pytest.mark.asyncio
    async def test_legitimate_compare_still_reaches_the_wire(self):
        gc, seen = _client()
        await gc.get_compare(REPO, "main", "job/abc")
        assert _wire_path(seen[-1]) == b"/api/v1/repos/srw/job-1/compare/main...job/abc"
