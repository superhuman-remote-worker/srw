"""Forge adapter — one pull-request call across GitHub, Gitea and GitLab.

Gitea deliberately mirrors GitHub's REST API, so those two differ only in
API base and auth scheme. GitLab is the outlier: a URL-encoded project path
instead of owner/repo segments, different field names, and its own header.

A per-repository token is the universal credential here — every forge
supports one, unlike GitHub Apps which are GitHub-only.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote, urlparse

import httpx

from shared.repo_path_safety import REPO_NAME_RE, check_repo_path_shape

logger = logging.getLogger(__name__)

SUPPORTED_FORGES = frozenset({"github", "gitea", "gitlab"})

# Overridden in tests with an httpx.MockTransport.
_transport: Optional[httpx.BaseTransport] = None


class ForgeError(RuntimeError):
    """Raised when a forge API call fails or is misconfigured."""


class ForgePathError(ForgeError, ValueError):
    """A connector-shaped owner, repository name or ref refused at the sink.

    Raised before any request leaves the process. ``owner``/``repo`` come from
    the connector's own URL (``parse_owner_repo``), and httpx normalises dot
    segments before sending: an unchecked ``..`` re-targets the request at a
    neighbouring repository and a raw ``?``/``#`` cuts the REST path short.
    Not a privilege boundary -- the token is the connector's own, so a
    traversal only reaches what it already reaches (unlike the admin Gitea
    client, ``orchestrator/services/gitea.py``) -- but a request must land on
    the repository the connector names or on none. A ``ForgeError``, so every
    existing caller already handles it.
    """


#: GitLab's path limit; GitHub and Gitea stop at 100, so no real name is longer.
_MAX_FORGE_NAME_LENGTH = 255


def validate_forge_name(name: str, *, kind: str = "repository") -> str:
    """Return ``name`` unchanged if it is a usable forge owner/repository name.

    The shared owner/repository charset (``REPO_NAME_RE``) admits exactly one
    URL path segment with nothing to encode, and ``.``/``..`` are the two
    spellings in it that are still dot segments. Unlike the admin Gitea sink,
    a name that merely *contains* ``..`` (``a..b``) passes: it is not a dot
    segment, and a connector names someone else's repository, not one SRW
    chose. ``kind`` only labels the error.
    """
    if not isinstance(name, str) or not name:
        raise ForgePathError(f"Forge {kind} name must be a non-empty string")
    if (
        len(name) > _MAX_FORGE_NAME_LENGTH
        or not REPO_NAME_RE.fullmatch(name)
        or name in (".", "..")
    ):
        raise ForgePathError(f"Forge {kind} name {name[:100]!r} is not allowed")
    return name


def _encode_ref(ref: str) -> str:
    """Validate a git ref and encode it as exactly one URL path segment.

    ``quote`` never encodes ``.``, so ``quote("..", safe="")`` is still a live
    dot segment: the shape has to be refused first (git itself forbids
    ``..``, control characters and empty components in ref names, so no
    legitimate ref is). Slashes then become ``%2F`` -- GitHub's
    ``git/trees/{sha}``, ``branches/{branch}`` and ``tarball/{ref}`` take the
    ref as one parameter.
    """
    if not isinstance(ref, str) or not ref:
        raise ForgePathError("Git ref must be a non-empty string")
    check_repo_path_shape(ref, what="Git ref", error=ForgePathError)
    return quote(ref, safe="")


def _repo_api_url(target: "ForgeRepo") -> str:
    """``{api_base}/repos/{owner}/{repo}`` -- the one GitHub/Gitea formatter.

    Re-validates the names rather than trusting ``ForgeRepo`` construction,
    so a descriptor forced past ``__post_init__`` still cannot reach a URL.
    """
    owner = validate_forge_name(target.owner, kind="owner")
    repo = validate_forge_name(target.repo)
    return f"{target.api_base}/repos/{owner}/{repo}"


def _hostname(value: str) -> str | None:
    """Return a normalized hostname without ever exposing URL userinfo."""

    raw = str(value or "").strip()
    prefix, separator, scp_path = raw.partition(":")
    if "//" not in raw and separator and "/" in scp_path and "/" not in prefix:
        host = prefix.rsplit("@", 1)[-1]
    else:
        host = urlparse(raw).hostname
    return str(host).casefold().rstrip(".") if host else None


def forge_web_url_matches_connector(
    web_url: str,
    connection_url: str,
    forge: str,
) -> bool:
    """Whether a forge-returned web URL belongs to the connector's host.

    Most connectors use the same browser and API hostname.  An in-cluster
    Gitea deployment deliberately does not: agents call ``GITEA_INTERNAL_URL``
    while Gitea returns links rooted at the browser-facing ``GITEA_URL``.
    Treat exactly that server-configured pair as aliases.  A connector for any
    other Gitea instance cannot borrow the deployment-wide public hostname.
    """

    web_host = _hostname(web_url)
    connection_host = _hostname(connection_url)
    if not web_host or not connection_host:
        return False
    if web_host == connection_host:
        return True
    if str(forge or "").strip().casefold() != "gitea":
        return False

    internal_host = _hostname(os.environ.get("GITEA_INTERNAL_URL", ""))
    public_host = _hostname(os.environ.get("GITEA_URL", ""))
    if not internal_host or not public_host:
        return False
    configured_hosts = {internal_host, public_host}
    return connection_host in configured_hosts and web_host in configured_hosts


@dataclass(frozen=True)
class ForgeRepo:
    """Everything needed to call one repository's API."""

    forge: str
    api_base: str
    owner: str
    repo: str
    # repr=False: one uncaught non-ForgeError renders this dataclass into a
    # traceback, and tracebacks reach logs and the agent transcript.
    token: str = field(repr=False)

    def __post_init__(self) -> None:
        # Refuse an unusable descriptor outright, so no URL builder -- present
        # or future -- ever sees one. Frozen: validate, never normalise.
        validate_forge_name(self.owner, kind="owner")
        validate_forge_name(self.repo)


class GitHubClient:
    """KB-sized GitHub API client with the native Gitea client's call shape.

    The KB writer commits exactly one note at a time, so GitHub's single-file
    Contents API is sufficient. Multi-file writes are rejected explicitly:
    implementing those atomically would require the Git Data API and is a
    separate design decision.
    """

    def __init__(self, target: ForgeRepo) -> None:
        if target.forge != "github":
            raise ForgeError("GitHubClient requires a github ForgeRepo")
        self._target = target

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._target.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _repo_api(self) -> str:
        return _repo_api_url(self._target)

    @staticmethod
    def _ref_segment(ref: str) -> str | None:
        try:
            return _encode_ref(ref)
        except ForgePathError:
            logger.warning("GitHub KB client refused an unusable git ref")
            return None

    def _matches_repo(self, repo_name: str) -> bool:
        if str(repo_name) == self._target.repo:
            return True
        logger.warning("GitHub KB client refused a mismatched repository name")
        return False

    @staticmethod
    def _content_path(path: str) -> str | None:
        candidate = str(path or "")
        if not candidate:
            return None
        try:
            check_repo_path_shape(candidate, what="Content path", error=ForgePathError)
        except ForgePathError:
            return None
        return "/".join(quote(part, safe="") for part in candidate.split("/"))

    async def list_tree(self, repo_name: str, ref: str) -> list[dict[str, str]] | None:
        """Return GitHub's recursive tree normalized to path/type/sha."""
        if not self._matches_repo(repo_name):
            return None
        safe_ref = self._ref_segment(ref)
        if safe_ref is None:
            return None
        try:
            async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
                response = await client.get(
                    f"{self._repo_api()}/git/trees/{safe_ref}",
                    params={"recursive": "1"},
                    headers=self._headers(),
                )
        except httpx.HTTPError as exc:
            logger.warning("GitHub KB tree request failed: %s", exc)
            return None
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            logger.warning(
                "GitHub KB tree request failed (HTTP %s)", response.status_code
            )
            return None
        try:
            payload = response.json()
            if payload.get("truncated"):
                # An incomplete tree would make the reindexer treat unseen
                # notes as deleted. Fail honestly instead.
                logger.warning("GitHub KB recursive tree was truncated")
                return None
            entries = payload.get("tree") or []
            return [
                {
                    "path": str(entry["path"]),
                    "type": str(entry["type"]),
                    "sha": str(entry["sha"]),
                }
                for entry in entries
                if isinstance(entry, dict)
                and entry.get("path") is not None
                and entry.get("type") is not None
                and entry.get("sha") is not None
            ]
        except (TypeError, ValueError, AttributeError, KeyError):
            logger.warning("GitHub KB tree response was malformed")
            return None

    async def change_files(
        self,
        repo_name: str,
        branch: str,
        files: list[dict],
        message: str,
    ) -> bool:
        """Create or update exactly one file through GitHub's Contents API."""
        if not self._matches_repo(repo_name):
            return False
        if not files:
            return True
        if len(files) != 1:
            logger.warning(
                "GitHub KB writes support exactly one file; refusing %d files",
                len(files),
            )
            return False

        file = files[0]
        path = self._content_path(str(file.get("path") or ""))
        operation = str(file.get("operation") or "create").lower()
        content_b64 = file.get("content_b64")
        if (
            path is None
            or operation not in {"create", "update"}
            or not isinstance(content_b64, str)
        ):
            logger.warning("GitHub KB write payload was invalid")
            return False

        contents_url = f"{self._repo_api()}/contents/{path}"
        sha = str(file.get("sha") or "").strip() or None
        try:
            async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
                if operation == "update" and sha is None:
                    lookup = await client.get(
                        contents_url,
                        params={"ref": branch},
                        headers=self._headers(),
                    )
                    if lookup.status_code != 200:
                        logger.warning(
                            "GitHub KB update SHA lookup failed (HTTP %s)",
                            lookup.status_code,
                        )
                        return False
                    value = lookup.json().get("sha")
                    sha = str(value).strip() if value else None
                    if sha is None:
                        logger.warning("GitHub KB update SHA lookup returned no SHA")
                        return False

                payload: dict[str, str] = {
                    "message": str(message),
                    "content": content_b64,
                    "branch": str(branch or "main"),
                }
                if operation == "update" and sha is not None:
                    payload["sha"] = sha
                response = await client.put(
                    contents_url,
                    headers=self._headers(),
                    json=payload,
                )
        except (httpx.HTTPError, ValueError, AttributeError) as exc:
            logger.warning("GitHub KB write failed: %s", exc)
            return False
        if response.status_code in (200, 201):
            return True
        logger.warning("GitHub KB write failed (HTTP %s)", response.status_code)
        return False

    async def delete_path(
        self,
        repo_name: str,
        branch: str,
        path: str,
        message: str,
        expected_sha: str | None = None,
    ) -> str:
        """Remove one file through the Contents API with compare-and-swap.

        Same verdict vocabulary as ``GiteaClient.delete_path``: ``deleted`` /
        ``absent`` / ``conflict`` / ``error``. GitHub requires the blob SHA on
        every delete and answers 409 when it no longer matches the branch —
        the lost-update guard the KB purge relies on. Without ``expected_sha``
        the current SHA is looked up first.
        """
        if not self._matches_repo(repo_name):
            return "error"
        content_path = self._content_path(path)
        if content_path is None:
            logger.warning("GitHub KB delete path was invalid")
            return "error"
        contents_url = f"{self._repo_api()}/contents/{content_path}"
        sha = str(expected_sha or "").strip() or None
        try:
            async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
                if sha is None:
                    lookup = await client.get(
                        contents_url,
                        params={"ref": branch},
                        headers=self._headers(),
                    )
                    if lookup.status_code == 404:
                        return "absent"
                    if lookup.status_code != 200:
                        logger.warning(
                            "GitHub KB delete SHA lookup failed (HTTP %s)",
                            lookup.status_code,
                        )
                        return "error"
                    value = lookup.json().get("sha")
                    sha = str(value).strip() if value else None
                    if sha is None:
                        return "error"
                response = await client.request(
                    "DELETE",
                    contents_url,
                    headers=self._headers(),
                    json={
                        "message": str(message),
                        "sha": sha,
                        "branch": str(branch or "main"),
                    },
                )
        except (httpx.HTTPError, ValueError, AttributeError) as exc:
            logger.warning("GitHub KB delete failed: %s", exc)
            return "error"
        if response.status_code == 200:
            return "deleted"
        if response.status_code == 404:
            return "absent"
        if response.status_code in (409, 422):
            logger.info("GitHub KB delete refused (HTTP %s)", response.status_code)
            return "conflict"
        logger.warning("GitHub KB delete failed (HTTP %s)", response.status_code)
        return "error"

    async def get_branch_head_sha(self, repo_name: str, branch: str) -> str | None:
        """Return the commit SHA at a GitHub branch head."""
        if not self._matches_repo(repo_name):
            return None
        safe_branch = self._ref_segment(branch)
        if safe_branch is None:
            return None
        try:
            async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
                response = await client.get(
                    f"{self._repo_api()}/branches/{safe_branch}",
                    headers=self._headers(),
                )
        except httpx.HTTPError as exc:
            logger.warning("GitHub KB branch request failed: %s", exc)
            return None
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            logger.warning(
                "GitHub KB branch request failed (HTTP %s)", response.status_code
            )
            return None
        try:
            value = response.json().get("commit", {}).get("sha")
            return str(value) if value else None
        except (TypeError, ValueError, AttributeError):
            logger.warning("GitHub KB branch response was malformed")
            return None

    async def download_repo_archive(
        self, repo_name: str, ref: str, dest_path: str
    ) -> bool:
        """Stream a GitHub tarball for ``ref`` to ``dest_path``."""
        if not self._matches_repo(repo_name):
            return False
        safe_ref = self._ref_segment(ref)
        if safe_ref is None:
            return False
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, read=120.0),
                transport=_transport,
                follow_redirects=True,
            ) as client:
                async with client.stream(
                    "GET",
                    f"{self._repo_api()}/tarball/{safe_ref}",
                    headers=self._headers(),
                ) as response:
                    if response.status_code != 200:
                        logger.warning(
                            "GitHub KB archive request failed (HTTP %s)",
                            response.status_code,
                        )
                        return False
                    with open(dest_path, "wb") as output:
                        async for chunk in response.aiter_bytes():
                            output.write(chunk)
            return True
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("GitHub KB archive request failed: %s", exc)
            return False


def parse_owner_repo(url: str) -> tuple[str, str]:
    """Return ``(owner, repo)`` from a repository URL."""
    raw = url.strip()
    prefix, separator, scp_path = raw.partition(":")
    if "://" not in raw and separator and "/" in scp_path and "/" not in prefix:
        path = scp_path.strip("/")
    else:
        path = urlparse(raw).path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        raise ForgeError(f"Cannot parse owner/repo from URL: {url!r}")
    # Trailing pair handles GitLab subgroups (group/sub/repo → sub, repo);
    # subgroup projects need the full path, handled in _gitlab_project_path.
    # A connector URL that yields ``..`` or ``x?y`` is misconfigured: refuse
    # it wherever it is parsed (save, probe, dispatch), not when a PR opens.
    return (
        validate_forge_name(parts[-2], kind="owner"),
        validate_forge_name(parts[-1]),
    )


def resolve_api_base(url: str, forge: str) -> str:
    """Return the API root for ``url`` on ``forge``."""
    if forge not in SUPPORTED_FORGES:
        raise ForgeError(
            f"Unsupported forge {forge!r}; expected one of {sorted(SUPPORTED_FORGES)}"
        )
    raw = url.strip()
    prefix, separator, scp_path = raw.partition(":")
    scp_host = None
    if "://" not in raw and separator and "/" in scp_path and "/" not in prefix:
        scp_host = prefix.rsplit("@", 1)[-1]

    parsed = urlparse(raw)
    hostname = scp_host or parsed.hostname
    if not hostname:
        raise ForgeError(f"Cannot parse host from URL: {url!r}")
    scheme = parsed.scheme if parsed.scheme in {"http", "https"} else "https"
    host_for_url = f"[{hostname}]" if ":" in hostname else hostname
    port = f":{parsed.port}" if not scp_host and parsed.port else ""
    origin = f"{scheme}://{host_for_url}{port}"

    if forge == "github":
        # github.com routes to the SaaS API host; Enterprise uses /api/v3.
        if hostname.lower() in ("github.com", "www.github.com"):
            return "https://api.github.com"
        return f"{origin}/api/v3"
    if forge == "gitea":
        return f"{origin}/api/v1"
    return f"{origin}/api/v4"


def _gitlab_project_path(owner: str, repo: str) -> str:
    """URL-encode ``owner/repo`` into a single path segment.

    GitLab's project endpoint takes an ID or a fully URL-encoded path. Passing
    it as two path segments silently 404s. Collapsing the pair into one
    segment keeps ``..`` from normalising by accident, not by intent, so the
    halves get the same name check as the GitHub/Gitea pair.
    """
    owner = validate_forge_name(owner, kind="owner")
    repo = validate_forge_name(repo)
    return quote(f"{owner}/{repo}", safe="")


def _request_for(
    target: ForgeRepo, *, title: str, head: str, base: str, body: str
) -> tuple[str, dict, dict]:
    """Return ``(url, headers, json_body)`` for this forge's PR endpoint."""
    if target.forge == "gitlab":
        url = (
            f"{target.api_base}/projects/"
            f"{_gitlab_project_path(target.owner, target.repo)}/merge_requests"
        )
        headers = {"PRIVATE-TOKEN": target.token}
        payload = {
            "title": title,
            "source_branch": head,
            "target_branch": base,
            "description": body,
        }
        return url, headers, payload

    url = f"{_repo_api_url(target)}/pulls"
    scheme = "token" if target.forge == "gitea" else "Bearer"
    headers = {
        "Authorization": f"{scheme} {target.token}",
        "Accept": "application/json",
    }
    payload = {"title": title, "head": head, "base": base, "body": body}
    return url, headers, payload


def _status_request_for(target: ForgeRepo, number: int) -> tuple[str, dict[str, str]]:
    """Return the forge-specific URL and headers for one PR status read."""
    if target.forge == "gitlab":
        url = (
            f"{target.api_base}/projects/"
            f"{_gitlab_project_path(target.owner, target.repo)}/"
            f"merge_requests/{number}"
        )
        headers = {"Accept": "application/json"}
        if target.token:
            headers["PRIVATE-TOKEN"] = target.token
        return url, headers

    url = f"{_repo_api_url(target)}/pulls/{number}"
    headers = {"Accept": "application/json"}
    if target.token:
        scheme = "token" if target.forge == "gitea" else "Bearer"
        headers["Authorization"] = f"{scheme} {target.token}"
    return url, headers


def _response_error_detail(response: httpx.Response) -> str:
    """Return a bounded forge error without ever inspecting request headers."""
    try:
        data = response.json()
    except ValueError:
        return ""
    if not isinstance(data, dict):
        return ""
    return str(data.get("message") or data.get("error") or "")[:200]


async def get_pull_request_status(target: ForgeRepo, number: int) -> dict:
    """Read and normalize one pull/merge request across supported forges.

    The stable state vocabulary is ``open | merged | closed``. GitHub and
    Gitea both report merged PRs as closed, so their explicit ``merged`` flag
    takes precedence over ``state``. GitLab reports merged directly and calls
    open merge requests ``opened``.
    """
    if target.forge not in SUPPORTED_FORGES:
        raise ForgeError(f"Unsupported forge {target.forge!r}")
    if isinstance(number, bool) or not isinstance(number, int) or number < 1:
        raise ForgeError("Pull request number must be a positive integer")

    url, headers = _status_request_for(target, number)
    try:
        async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
            response = await client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        raise ForgeError(f"Could not reach {target.forge}: {exc}") from exc

    if response.status_code != 200:
        detail = _response_error_detail(response)
        raise ForgeError(
            f"{target.forge} refused the pull request status read "
            f"(HTTP {response.status_code}){': ' + detail if detail else ''}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise ForgeError(f"{target.forge} returned malformed PR status JSON") from exc
    if not isinstance(data, dict):
        raise ForgeError(f"{target.forge} returned malformed PR status data")

    remote_number = data.get("number", data.get("iid"))
    if remote_number is not None:
        try:
            if int(remote_number) != number:
                raise ForgeError(
                    f"{target.forge} returned a different pull request number"
                )
        except (TypeError, ValueError) as exc:
            raise ForgeError(
                f"{target.forge} returned a malformed pull request number"
            ) from exc

    raw_state = str(data.get("state") or "").strip().lower()
    if target.forge == "gitlab":
        state = {
            "opened": "open",
            "merged": "merged",
            "closed": "closed",
            "locked": "closed",
        }.get(raw_state)
        head = data.get("source_branch")
        base = data.get("target_branch")
        diff_refs = data.get("diff_refs")
        diff_refs = diff_refs if isinstance(diff_refs, dict) else {}
        head_sha = data.get("sha") or diff_refs.get("head_sha")
    else:
        if data.get("merged") is True or data.get("merged_at"):
            state = "merged"
        else:
            state = {"open": "open", "closed": "closed"}.get(raw_state)
        raw_head = data.get("head")
        raw_base = data.get("base")
        head = raw_head.get("ref") if isinstance(raw_head, dict) else None
        base = raw_base.get("ref") if isinstance(raw_base, dict) else None
        head_sha = raw_head.get("sha") if isinstance(raw_head, dict) else None
    if state is None:
        raise ForgeError(
            f"{target.forge} returned an unknown pull request state {raw_state!r}"
        )

    link = data.get("html_url") or data.get("web_url") or ""
    return {
        "number": number,
        "url": str(link),
        "state": state,
        "head": str(head or ""),
        "base": str(base or ""),
        "head_sha": str(head_sha or "").strip().lower(),
        "draft": bool(data.get("draft") or data.get("work_in_progress")),
    }


async def open_pull_request(
    target: ForgeRepo, *, title: str, head: str, base: str, body: str = ""
) -> dict:
    """Open a pull/merge request. Returns ``{"number": int, "url": str}``."""
    if target.forge not in SUPPORTED_FORGES:
        raise ForgeError(f"Unsupported forge {target.forge!r}")

    url, headers, payload = _request_for(
        target, title=title, head=head, base=base, body=body
    )

    try:
        async with httpx.AsyncClient(timeout=30.0, transport=_transport) as client:
            resp = await client.post(url, headers=headers, json=payload)
    except httpx.HTTPError as exc:
        raise ForgeError(f"Could not reach {target.forge}: {exc}") from exc

    if resp.status_code not in (200, 201):
        # Include the forge's message but never the request headers/token.
        detail = ""
        try:
            data = resp.json()
            detail = str(data.get("message") or data.get("error") or "")[:200]
            # GitHub/Gitea bury the actionable reason one level down: the two
            # most common PR refusals ("head branch not pushed", "no commits
            # between X and Y") both arrive as a generic
            # {"message": "Validation Failed", "errors": [{"message": ...}]}.
            errors = data.get("errors")
            if isinstance(errors, list) and errors:
                first = errors[0]
                nested = first.get("message") if isinstance(first, dict) else first
                nested = str(nested or "")[:200]
                if nested and nested != detail:
                    detail = f"{detail}: {nested}" if detail else nested
        except (ValueError, AttributeError):
            pass
        raise ForgeError(
            f"{target.forge} refused the pull request "
            f"(HTTP {resp.status_code}){': ' + detail if detail else ''}"
        )

    data = resp.json()
    # GitHub/Gitea: number + html_url. GitLab: iid + web_url.
    number = data.get("number", data.get("iid"))
    link = data.get("html_url") or data.get("web_url") or ""
    if number is None:
        raise ForgeError(f"{target.forge} returned no PR number: {list(data)[:5]}")
    return {"number": int(number), "url": link}


# GitLab access levels: 30 = Developer (may push), 40 = Maintainer, 50 = Owner.
_GITLAB_WRITE_LEVEL = 30
_GITLAB_ADMIN_LEVEL = 40


def _probe_requests_for(
    target: ForgeRepo,
) -> tuple[tuple[str, dict[str, str]], tuple[str, dict[str, str]]]:
    """Return ``((user_url, headers), (repo_url, headers))`` for one probe."""
    if target.forge == "gitlab":
        headers = {"PRIVATE-TOKEN": target.token, "Accept": "application/json"}
        repo_url = (
            f"{target.api_base}/projects/"
            f"{_gitlab_project_path(target.owner, target.repo)}"
        )
        return (f"{target.api_base}/user", headers), (repo_url, headers)

    scheme = "token" if target.forge == "gitea" else "Bearer"
    headers = {
        "Authorization": f"{scheme} {target.token}",
        "Accept": "application/json",
    }
    repo_url = _repo_api_url(target)
    return (f"{target.api_base}/user", headers), (repo_url, headers)


async def probe_repository_access(target: ForgeRepo, *, timeout: float = 10.0) -> dict:
    """Authenticate the connector token and report who it is and what it may do.

    Two reads, nothing written: the token's own principal (``/user``) and the
    repository's permission view. Raises :class:`ForgeError` when the token
    is rejected, the repository is invisible, or the forge is unreachable.

    The facts exist so an operator can verify a connector without ever
    reading the token: which account the agent will act as, whether that
    account administers the repository (branch rules with an admin bypass
    then do not bind the agent), whether the token is a classic scoped PAT
    (GitHub only reveals this), and the repository's real default branch.
    Warnings that need connector context (read-only flag, configured
    branch) are the caller's to add.
    """
    if target.forge not in SUPPORTED_FORGES:
        raise ForgeError(f"Unsupported forge {target.forge!r}")
    if not target.token:
        raise ForgeError("Repository connector has no token to probe")

    (user_url, user_headers), (repo_url, repo_headers) = _probe_requests_for(target)
    try:
        async with httpx.AsyncClient(timeout=timeout, transport=_transport) as client:
            user_resp = await client.get(user_url, headers=user_headers)
            if user_resp.status_code == 401:
                raise ForgeError(f"{target.forge} rejected the token (HTTP 401)")
            if user_resp.status_code != 200:
                detail = _response_error_detail(user_resp)
                raise ForgeError(
                    f"{target.forge} could not identify the token "
                    f"(HTTP {user_resp.status_code}){': ' + detail if detail else ''}"
                )
            repo_resp = await client.get(repo_url, headers=repo_headers)
    except httpx.HTTPError as exc:
        raise ForgeError(f"Could not reach {target.forge}: {exc}") from exc

    if repo_resp.status_code == 404:
        raise ForgeError(
            f"{target.owner}/{target.repo} not found on {target.forge}, or the "
            "token cannot see it (HTTP 404)"
        )
    if repo_resp.status_code != 200:
        detail = _response_error_detail(repo_resp)
        raise ForgeError(
            f"{target.forge} refused the repository read "
            f"(HTTP {repo_resp.status_code}){': ' + detail if detail else ''}"
        )

    try:
        user = user_resp.json()
        repo = repo_resp.json()
    except ValueError as exc:
        raise ForgeError(f"{target.forge} returned a non-JSON probe response") from exc
    if not isinstance(user, dict) or not isinstance(repo, dict):
        raise ForgeError(f"{target.forge} returned an unexpected probe response shape")

    scopes: list[str] | None = None
    if target.forge == "gitlab":
        principal = str(user.get("username") or "")
        perms = repo.get("permissions") or {}
        levels = [
            (perms.get(key) or {}).get("access_level")
            for key in ("project_access", "group_access")
            if isinstance(perms.get(key), dict)
        ]
        level = max((int(v) for v in levels if isinstance(v, int)), default=0)
        can_write = level >= _GITLAB_WRITE_LEVEL
        is_admin = level >= _GITLAB_ADMIN_LEVEL
        token_class = "unknown"
    else:
        principal = str(user.get("login") or "")
        perms = repo.get("permissions") or {}
        can_write = bool(perms.get("push"))
        is_admin = bool(perms.get("admin"))
        if target.forge == "github":
            # Classic PATs answer with X-OAuth-Scopes; fine-grained PATs and
            # App installation tokens never send the header.
            scopes_header = user_resp.headers.get("x-oauth-scopes")
            if scopes_header is None:
                token_class = "fine-grained"
            else:
                token_class = "classic"
                scopes = [s.strip() for s in scopes_header.split(",") if s.strip()]
        else:
            token_class = "unknown"

    warnings: list[str] = []
    if is_admin:
        warnings.append(
            f"{principal or 'the token principal'} administers "
            f"{target.owner}/{target.repo}: branch rules with an admin bypass "
            "will not bind the agent"
        )
    if scopes is not None and "repo" in scopes:
        warnings.append(
            "classic 'repo' scope reaches every repository this account can "
            "push to, private ones included; 'public_repo' bounds it to public repos"
        )

    return {
        "forge": target.forge,
        "owner": target.owner,
        "repo": target.repo,
        "principal": principal,
        "principal_id": user.get("id"),
        "token_class": token_class,
        "scopes": scopes,
        "can_write": can_write,
        "is_admin": is_admin,
        "default_branch": str(repo.get("default_branch") or "") or None,
        "warnings": warnings,
    }
