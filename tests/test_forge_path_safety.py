"""Connector-shaped names, refs and paths are refused at the forge sink.

The repository-connector forge adapter builds ``{api_base}/repos/{owner}/{repo}``
(GitHub/Gitea) and ``/projects/{owner%2Frepo}`` (GitLab) from the pair
``parse_owner_repo`` cuts out of the connector's own URL. httpx normalises dot
segments before sending, so an ``owner`` or ``repo`` of ``..`` re-targets the
request at a different repository, and a raw ``?``/``#`` cuts the REST path
short. ``urllib.parse.quote`` never encodes ``.``, so the GitHub client's
``quote(..., safe="")`` was no defence against a name that already *is* a dot
segment.

Low severity -- the token is the connector's own, so there is no boundary to
cross -- but the same sink discipline as the admin Gitea client
(``tests/test_gitea_path_safety.py``): an unusable name cannot be constructed,
parsed out of a URL, or reach a URL formatter, and nothing is sent. The
clients run over a real ``httpx.MockTransport`` so the assertions are on the
bytes that would leave the process.
"""

import dataclasses

import httpx
import pytest

from shared.runtime.services import forge
from shared.runtime.services.forge import (
    ForgeError,
    ForgePathError,
    ForgeRepo,
    GitHubClient,
    get_pull_request_status,
    open_pull_request,
    parse_owner_repo,
    probe_repository_access,
    validate_forge_name,
)

# The brief's table plus the dotted/encoded variants that reach the same place
# after one normalisation or one decode.
BAD_NAMES = [
    "..",
    ".",
    "",
    "../x",
    "a/b",
    "a\\b",
    "%2e%2e",
    "a%2Fb",
    "x?y",
    "x#y",
    "a b",
    "a:b",
    "x\x00",
    "ü",
    "a" * 256,
]
# Real owner/repository names on GitHub, Gitea and GitLab: ``.github`` is the
# org-profile repository, and ``a..b`` is not a dot segment (the admin Gitea
# sink refuses it only because SRW names every repository it manages).
GOOD_NAMES = ["acme", "widget", "a.b-c_d", ".github", "Widget2", "a..b", "a" * 100]

REF_ESCAPES = ["..", ".", "../x", "..%2Fx", "%2e%2e", "/main", "a//b", "x\x00", ""]
PATH_ESCAPES = [
    "../x.md",
    "%2e%2e/x.md",
    "..%2Fx.md",
    "a/../../b.md",
    "/etc/passwd",
    "a//b.md",
    "a\\b.md",
    "notes/x\x00.md",
    "./notes/x.md",
]

API_BASES = {
    "github": "https://api.github.com",
    "gitea": "https://git.example.test/api/v1",
    "gitlab": "https://gitlab.example.test/api/v4",
}


def _repo(forge_name: str = "github", owner: str = "acme", repo: str = "widget"):
    return ForgeRepo(forge_name, API_BASES[forge_name], owner, repo, "tok")


def _forced(forge_name: str, *, owner: str = "acme", repo: str = "widget"):
    """A descriptor whose names were forced past construction-time validation.

    Proves every URL formatter re-checks at the sink instead of trusting that
    ``ForgeRepo`` was built through ``__init__``.
    """
    target = _repo(forge_name)
    object.__setattr__(target, "owner", owner)
    object.__setattr__(target, "repo", repo)
    return target


@pytest.fixture
def wire(monkeypatch):
    """Route every forge request through a recording MockTransport."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "number": 7,
                "html_url": "https://github.com/acme/widget/pull/7",
                "state": "open",
                "login": "bot",
                "permissions": {"push": True},
                "commit": {"sha": "a" * 40},
                "tree": [],
            },
        )

    monkeypatch.setattr(forge, "_transport", httpx.MockTransport(handler))
    return seen


def _wire_path(request: httpx.Request) -> bytes:
    """The percent-encoded path exactly as it would leave the process."""
    return request.url.raw_path.split(b"?", 1)[0]


# ---------------------------------------------------------------------------
# The name rule
# ---------------------------------------------------------------------------


class TestValidateForgeName:
    @pytest.mark.parametrize("name", BAD_NAMES)
    def test_rejects(self, name):
        with pytest.raises(ForgePathError):
            validate_forge_name(name)

    @pytest.mark.parametrize("name", GOOD_NAMES)
    def test_accepts(self, name):
        assert validate_forge_name(name) == name

    def test_non_string_is_refused(self):
        with pytest.raises(ForgePathError):
            validate_forge_name(None)  # type: ignore[arg-type]

    def test_is_a_forge_error_every_caller_already_handles(self):
        # open_pull_request / probe / status callers all catch ForgeError.
        assert issubclass(ForgePathError, ForgeError)
        assert issubclass(ForgePathError, ValueError)


class TestForgeRepoConstruction:
    @pytest.mark.parametrize("name", BAD_NAMES)
    @pytest.mark.parametrize("field", ["owner", "repo"])
    def test_unusable_descriptor_cannot_be_built(self, field, name):
        kwargs = {"owner": "acme", "repo": "widget", field: name}
        with pytest.raises(ForgePathError):
            ForgeRepo("github", API_BASES["github"], token="tok", **kwargs)

    def test_replace_goes_through_the_same_check(self):
        with pytest.raises(ForgePathError):
            dataclasses.replace(_repo(), repo="..")

    def test_refused_name_never_echoes_the_token(self):
        with pytest.raises(ForgePathError) as exc:
            ForgeRepo("github", API_BASES["github"], "..", "widget", "sekrit-token")
        assert "sekrit-token" not in str(exc.value)


class TestParseOwnerRepo:
    @pytest.mark.parametrize(
        "url",
        [
            "https://git.example.test/../..",
            "https://git.example.test/acme/..",
            "https://git.example.test/acme/%2e%2e",
            "https://git.example.test/acme/x%3Fy",
            "https://git.example.test/acme/x%23y",
            "https://git.example.test/a%2Fb/widget",
            "git@git.example.test:acme/x?y.git",
            "git@git.example.test:acme/x#y.git",
            "git@git.example.test:../...git",
        ],
    )
    def test_connector_url_that_parses_to_an_unusable_name_is_refused(self, url):
        with pytest.raises(ForgePathError):
            parse_owner_repo(url)

    @pytest.mark.parametrize(
        ("url", "pair"),
        [
            ("https://github.com/acme/widget.git", ("acme", "widget")),
            ("https://github.com/acme/.github", ("acme", ".github")),
            ("git@github.com:acme/a.b-c_d.git", ("acme", "a.b-c_d")),
            # A real query/fragment is not part of the path at all.
            ("https://github.com/acme/widget?tab=readme#top", ("acme", "widget")),
        ],
    )
    def test_real_urls_still_parse(self, url, pair):
        assert parse_owner_repo(url) == pair


# ---------------------------------------------------------------------------
# Every URL formatter re-checks at the sink
# ---------------------------------------------------------------------------


class TestUrlFormattersRefuseAtTheSink:
    @pytest.mark.parametrize("forge_name", ["github", "gitea", "gitlab"])
    @pytest.mark.parametrize("name", ["..", "x?y", "x#y", "a/b"])
    def test_pr_create_status_and_probe_builders(self, forge_name, name):
        for field in ("owner", "repo"):
            target = _forced(forge_name, **{field: name})
            with pytest.raises(ForgePathError):
                forge._request_for(target, title="t", head="h", base="b", body="")
            with pytest.raises(ForgePathError):
                forge._status_request_for(target, 7)
            with pytest.raises(ForgePathError):
                forge._probe_requests_for(target)

    @pytest.mark.parametrize("name", ["..", "x?y", "%2e%2e"])
    def test_github_client_repo_api(self, name):
        client = GitHubClient(_forced("github", owner=name))
        with pytest.raises(ForgePathError):
            client._repo_api()

    @pytest.mark.parametrize(
        ("forge_name", "expected"),
        [
            ("github", "https://api.github.com/repos/acme/widget/pulls"),
            ("gitea", "https://git.example.test/api/v1/repos/acme/widget/pulls"),
            (
                "gitlab",
                "https://gitlab.example.test/api/v4/projects/acme%2Fwidget/merge_requests",
            ),
        ],
    )
    def test_valid_names_keep_their_exact_urls(self, forge_name, expected):
        url, _headers, _body = forge._request_for(
            _repo(forge_name), title="t", head="h", base="b", body=""
        )
        assert url == expected
        status_url, _ = forge._status_request_for(_repo(forge_name), 7)
        assert status_url == expected + "/7"
        (_user, _), (repo_url, _) = forge._probe_requests_for(_repo(forge_name))
        assert repo_url == expected.rsplit("/", 1)[0]


# ---------------------------------------------------------------------------
# Nothing leaves the process
# ---------------------------------------------------------------------------


class TestNothingIsSent:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("forge_name", ["github", "gitea", "gitlab"])
    async def test_pr_open_status_and_probe(self, wire, forge_name):
        target = _forced(forge_name, repo="..")
        with pytest.raises(ForgeError):
            await open_pull_request(target, title="t", head="h", base="b")
        with pytest.raises(ForgeError):
            await get_pull_request_status(target, 7)
        with pytest.raises(ForgeError):
            await probe_repository_access(target)
        assert wire == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ref", REF_ESCAPES)
    async def test_github_client_refs(self, wire, tmp_path, ref):
        client = GitHubClient(_repo())
        assert await client.list_tree("widget", ref) is None
        assert await client.get_branch_head_sha("widget", ref) is None
        dest = tmp_path / "archive.tar.gz"
        assert await client.download_repo_archive("widget", ref, str(dest)) is False
        assert not dest.exists()
        assert wire == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", PATH_ESCAPES)
    async def test_github_client_content_paths(self, wire, path):
        client = GitHubClient(_repo())
        files = [{"path": path, "operation": "create", "content_b64": "aGk="}]
        assert await client.change_files("widget", "main", files, "m") is False
        assert await client.delete_path("widget", "main", path, "m", "abc") == "error"
        assert wire == []


class TestValidValuesLeaveEncoded:
    @pytest.mark.asyncio
    async def test_slashed_branch_travels_as_one_segment(self, wire):
        client = GitHubClient(_repo())
        assert await client.get_branch_head_sha("widget", "job/abc") == "a" * 40
        assert _wire_path(wire[-1]) == b"/repos/acme/widget/branches/job%2Fabc"

    @pytest.mark.asyncio
    async def test_tree_ref(self, wire):
        client = GitHubClient(_repo())
        assert await client.list_tree("widget", "a" * 40) == []
        assert _wire_path(wire[-1]) == b"/repos/acme/widget/git/trees/" + b"a" * 40

    @pytest.mark.asyncio
    async def test_content_path_is_encoded_per_segment(self, wire):
        client = GitHubClient(_repo())
        files = [
            {
                "path": "notes/sub dir/файл #1?.md",
                "operation": "create",
                "content_b64": "aGk=",
            }
        ]
        assert await client.change_files("widget", "main", files, "m") is True
        assert _wire_path(wire[-1]) == (
            b"/repos/acme/widget/contents/notes/sub%20dir/"
            b"%D1%84%D0%B0%D0%B9%D0%BB%20%231%3F.md"
        )

    @pytest.mark.asyncio
    async def test_dotfile_owner_repo_reaches_the_wire_unchanged(self, wire):
        await probe_repository_access(_repo("github", repo=".github"))
        assert _wire_path(wire[-1]) == b"/repos/acme/.github"
