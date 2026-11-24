"""Unit tests for ``NextcloudBackend``'s byte-level project-folder methods.

Stands the adapter up against an ``httpx.MockTransport`` backed by a small
in-memory WebDAV server (``FakeNextcloud``) that speaks the Group Folders
DAV endpoint (``/remote.php/dav/groupfolders/<user>/<mountpoint>/``). Covers
the four methods that back Mode-A job cloud export: ``list_project_folder``
(recursive ``Depth: 1`` walk), ``get_project_folder_file_bytes``,
``put_project_folder_file_bytes`` (PUT, MKCOL-ing parents only when the PUT
finds one missing), and
``delete_project_folder_file``.

The fake's PROPFIND bodies mirror the real sabre/dav multistatus shape
captured from a live Nextcloud (``xmlns:s="http://sabredav.org/ns"``,
entity-encoded etags, server-absolute URL-encoded hrefs) so the shared
``parse_propfind_entries`` is exercised against a realistic body.
"""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, quote, unquote

import httpx
import pytest

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    CloudMountSubject,
    NextcloudBackend,
    ProjectFolderHandle,
    SessionFolderHandle,
)
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.nextcloud import share_rate_bucket
from orchestrator.services.cloud.folder_grants import principal_ids

NEXTCLOUD_BASE = "https://nc.example.com"
AGENT_USER = "agent-service"
_WRITER = principal_ids("writer", "11111111-1111-4111-8111-111111111111")[0]
_READER = principal_ids("reader", "11111111-1111-4111-8111-111111111111")[0]
# Mountpoint with a space → exercises URL-encoding end-to-end (the real
# "NC Validation Project" group folder has one too).
MOUNTPOINT = "Test Project"


def _nc_test_settings() -> NextcloudSettings:
    """Minimal valid ``NextcloudSettings`` for constructing the backend.

    Only ``agent_user`` (→ the groupfolders DAV path) actually matters for
    routing; the fake transport is injected via ``_install_fake`` afterwards.
    """
    return NextcloudSettings(
        base_url=NEXTCLOUD_BASE,
        public_url=NEXTCLOUD_BASE,
        admin_user="admin",
        admin_password="admin",
        agent_user=AGENT_USER,
        agent_password="agent-service-dev",
    )


def _base_prefix() -> str:
    return f"/remote.php/dav/groupfolders/{AGENT_USER}/{quote(MOUNTPOINT, safe='')}"


def _handle(*, mountpoint: str | None = MOUNTPOINT) -> ProjectFolderHandle:
    meta = {"mountpoint": mountpoint} if mountpoint is not None else {}
    return ProjectFolderHandle(backend="nextcloud", native_id="2", vendor_meta=meta)


# ----------------------------------------------------------------------- Fake


class FakeNextcloud:
    """In-memory WebDAV stand-in for a Nextcloud Group Folder.

    Stores file bytes + directories relative to the folder root and answers
    PROPFIND (Depth: 1), GET, PUT, MKCOL, DELETE against the groupfolders
    DAV endpoint.
    """

    def __init__(self) -> None:
        self.base = _base_prefix()
        self.decoded_base = unquote(self.base)
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = set()
        self.etags: dict[str, str] = {}
        self.requests: list[httpx.Request] = []
        self._etag_seq = 0
        # When True, a ``Depth: infinity`` PROPFIND is answered 400 (mimics
        # sabre/dav with infinity disabled) so the etag baseline falls back to
        # the Depth:1 BFS. Default False: the fake honors infinity.
        self.reject_infinity = False
        # What a PUT into a missing collection answers: 409 in plain
        # sabre/dav, 404 from a real Nextcloud 31.
        self.missing_parent_put_status = 409

    # ---- seeding helper
    def add_file(self, relpath: str, content: bytes) -> None:
        relpath = relpath.strip("/")
        self.files[relpath] = content
        self.etags[relpath] = self._next_etag()
        parts = relpath.split("/")[:-1]
        for i in range(len(parts)):
            self.dirs.add("/".join(parts[: i + 1]))

    def _next_etag(self) -> str:
        self._etag_seq += 1
        return f"etag{self._etag_seq}"

    # ---- dispatch
    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        rel = self._rel(request)
        if rel is None:
            return httpx.Response(404, content=b"outside group folder")
        method = request.method
        if method == "PROPFIND":
            return self._propfind(rel, request.headers.get("Depth", "1"))
        if method == "GET":
            return self._get(rel)
        if method == "PUT":
            return self._put(rel, bytes(request.content))
        if method == "MKCOL":
            return self._mkcol(rel)
        if method == "DELETE":
            return self._delete(rel)
        return httpx.Response(405, content=f"unexpected {method}".encode())

    def _rel(self, request: httpx.Request) -> str | None:
        path = unquote(request.url.path)
        if not path.startswith(self.decoded_base):
            return None
        return path[len(self.decoded_base) :].strip("/")

    # ---- WebDAV verbs
    def _propfind(self, rel: str, depth: str = "1") -> httpx.Response:
        if rel and rel not in self.dirs:
            return httpx.Response(404, content=self._dav_error())
        if depth == "infinity":
            if self.reject_infinity:
                return httpx.Response(400, content=self._dav_error())
            members = self._descendants(rel)
        else:
            members = self._children(rel)
        # The collection itself + the selected members (children or all descendants).
        blocks = [self._block(rel, is_dir=True)]
        for child, is_dir in sorted(members.items()):
            blocks.append(self._block(child, is_dir=is_dir))
        body = (
            '<?xml version="1.0"?>'
            '<d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" '
            'xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">'
            + "".join(blocks)
            + "</d:multistatus>"
        )
        return httpx.Response(
            207,
            content=body.encode(),
            headers={"Content-Type": "application/xml; charset=utf-8"},
        )

    def _get(self, rel: str) -> httpx.Response:
        if rel in self.files:
            return httpx.Response(
                200,
                content=self.files[rel],
                headers={"Content-Type": self._ctype(rel)},
            )
        return httpx.Response(404, content=self._dav_error())

    def _put(self, rel: str, content: bytes) -> httpx.Response:
        if not rel:
            return httpx.Response(409, content=self._dav_error())
        parent = "/".join(rel.split("/")[:-1])
        if parent and parent not in self.dirs:
            return httpx.Response(
                self.missing_parent_put_status, content=self._dav_error()
            )
        self.files[rel] = content
        self.etags[rel] = self._next_etag()
        return httpx.Response(201)

    def _mkcol(self, rel: str) -> httpx.Response:
        if not rel or rel in self.dirs:
            return httpx.Response(405)  # exists / not allowed on root
        parent = "/".join(rel.split("/")[:-1])
        if parent and parent not in self.dirs:
            return httpx.Response(409, content=self._dav_error())
        self.dirs.add(rel)
        return httpx.Response(201)

    def _delete(self, rel: str) -> httpx.Response:
        if rel in self.files:
            del self.files[rel]
            self.etags.pop(rel, None)
            return httpx.Response(204)
        if rel in self.dirs:
            self.dirs.discard(rel)
            return httpx.Response(204)
        return httpx.Response(404, content=self._dav_error())

    # ---- helpers
    def _descendants(self, current: str) -> dict[str, bool]:
        """Every file + dir at any depth under ``current`` → {relpath: is_dir}
        (for a ``Depth: infinity`` PROPFIND)."""
        prefix = (current + "/") if current else ""
        members: dict[str, bool] = {}
        for f in self.files:
            if f.startswith(prefix) and f != current:
                members[f] = False
        for d in self.dirs:
            if d.startswith(prefix) and d != current:
                members[d] = True
        return members

    def _children(self, current: str) -> dict[str, bool]:
        """Immediate children of ``current`` → {relpath: is_dir}."""
        prefix = (current + "/") if current else ""
        children: dict[str, bool] = {}
        for f in self.files:
            if f.startswith(prefix):
                rest = f[len(prefix) :]
                if "/" not in rest:
                    children[f] = False
                else:
                    children[prefix + rest.split("/", 1)[0]] = True
        for d in self.dirs:
            if d.startswith(prefix):
                rest = d[len(prefix) :]
                if not rest:
                    continue
                if "/" not in rest:
                    children[d] = True
                else:
                    children[prefix + rest.split("/", 1)[0]] = True
        return children

    def _href(self, rel: str, is_dir: bool) -> str:
        if not rel:
            return f"{self.base}/"
        href = f"{self.base}/{quote(rel, safe='/')}"
        return href + "/" if is_dir else href

    def _block(self, rel: str, *, is_dir: bool) -> str:
        if is_dir:
            rtype = "<d:resourcetype><d:collection/></d:resourcetype>"
            size = ""
            ctype = ""
            etag_val = f"d-{rel or 'root'}"
        else:
            rtype = "<d:resourcetype/>"
            size = f"<d:getcontentlength>{len(self.files.get(rel, b''))}</d:getcontentlength>"
            ctype = f"<d:getcontenttype>{self._ctype(rel)}</d:getcontenttype>"
            etag_val = self.etags.get(rel, "0")
        return (
            "<d:response>"
            f"<d:href>{self._href(rel, is_dir)}</d:href>"
            "<d:propstat><d:prop>"
            f"{rtype}{size}{ctype}"
            f"<d:getetag>&quot;{etag_val}&quot;</d:getetag>"
            "</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>"
            "</d:response>"
        )

    @staticmethod
    def _ctype(rel: str) -> str:
        if rel.endswith((".txt", ".md")):
            return "text/plain"
        return "application/octet-stream"

    @staticmethod
    def _dav_error() -> bytes:
        return (
            b'<?xml version="1.0" encoding="utf-8"?>'
            b'<d:error xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns">'
            b"<s:exception>Sabre\\DAV\\Exception\\NotFound</s:exception></d:error>"
        )


class TestRemoveUserFromGroup:
    @pytest.mark.asyncio
    async def test_issues_delete_with_body_without_typeerror(self):
        # Regression: httpx's AsyncClient.delete() rejects a request body, so
        # the old `delete(..., data=...)` raised TypeError. It must send the
        # groupid via a DELETE request body.
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["method"] = request.method
            seen["path"] = request.url.path
            seen["content"] = request.content
            return httpx.Response(200, json={"ocs": {"meta": {"statuscode": 100}}})

        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(handler)
        )
        be._initialized = True

        await be.remove_user_from_group("srw-reader-a", "grp1")  # must not raise

        assert seen["method"] == "DELETE"
        assert seen["path"] == "/ocs/v2.php/cloud/users/srw-reader-a/groups"
        assert b"grp1" in seen["content"]  # groupid sent in the request body


class TestEnsureSessionFolderExclusive:
    """An exclusive create refuses a folder that exists already (the job
    outbox copies only into a folder of its own); a plain one reuses it."""

    def _backend(self, existing: set[str]) -> tuple[NextcloudBackend, list[str]]:
        made: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "MKCOL"
            path = unquote(request.url.path).split(f"/files/{AGENT_USER}/", 1)[1]
            if path in existing:
                return httpx.Response(405)
            made.append(path)
            return httpx.Response(201)

        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(handler)
        )
        be._agent_client = be._build_agent_client(httpx.MockTransport(handler))
        be._initialized = True
        return be, made

    @pytest.mark.asyncio
    async def test_an_existing_folder_is_refused_only_when_exclusive(self):
        be, _made = self._backend({"sessions", "sessions/report-1"})

        with pytest.raises(CloudBackendError) as ei:
            await be.ensure_session_folder(session_id="report-1", exclusive=True)
        assert ei.value.kind is CloudBackendErrorKind.ALREADY_EXISTS

        handle = await be.ensure_session_folder(session_id="report-1")
        assert handle.native_id == "sessions/report-1"

    @pytest.mark.asyncio
    async def test_an_existing_parent_never_refuses(self):
        be, made = self._backend({"sessions"})

        handle = await be.ensure_session_folder(session_id="report-2", exclusive=True)

        assert handle.native_id == "sessions/report-2"
        assert made == ["sessions/report-2"]


class TestSessionFolderLeafFirst:
    """A session folder costs one MKCOL when ``sessions/`` is there; the
    parent is created only when the leaf's MKCOL says it is missing (409),
    and a 409 never reads as "the folder exists"."""

    def _backend(
        self, dirs: set[str], *, leaf_conflicts: int = 0
    ) -> tuple[NextcloudBackend, list[tuple[str, str]]]:
        seen: list[tuple[str, str]] = []
        conflicts = {"left": leaf_conflicts}

        def handler(request: httpx.Request) -> httpx.Response:
            path = unquote(request.url.path).split(f"/files/{AGENT_USER}/", 1)[1]
            seen.append((request.method, path))
            if request.method == "PUT":
                parent = path.rsplit("/", 1)[0]
                return httpx.Response(201 if parent in dirs else 404)
            assert request.method == "MKCOL"
            if path in dirs:
                return httpx.Response(405)
            parent = path.rsplit("/", 1)[0] if "/" in path else ""
            if parent and parent not in dirs:
                return httpx.Response(409)
            if parent and conflicts["left"]:
                conflicts["left"] -= 1
                return httpx.Response(409)
            dirs.add(path)
            return httpx.Response(201)

        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(handler)
        )
        be._agent_client = be._build_agent_client(httpx.MockTransport(handler))
        be._initialized = True
        return be, seen

    @pytest.mark.asyncio
    async def test_an_existing_parent_costs_exactly_one_mkcol(self):
        be, seen = self._backend({"sessions"})
        handle = await be.ensure_session_folder(session_id="s-1", exclusive=True)
        assert handle.native_id == "sessions/s-1"
        assert seen == [("MKCOL", "sessions/s-1")]

    @pytest.mark.asyncio
    async def test_a_missing_parent_is_created_and_the_leaf_retried_once(self):
        dirs: set[str] = set()
        be, seen = self._backend(dirs)
        await be.ensure_session_folder(session_id="s-1", exclusive=True)
        assert seen == [
            ("MKCOL", "sessions/s-1"),
            ("MKCOL", "sessions"),
            ("MKCOL", "sessions/s-1"),
        ]
        assert dirs == {"sessions", "sessions/s-1"}

    @pytest.mark.asyncio
    async def test_a_second_conflict_is_unavailable_never_already_exists(self):
        be, seen = self._backend(set(), leaf_conflicts=1)
        with pytest.raises(CloudBackendError) as ei:
            await be.ensure_session_folder(session_id="s-1", exclusive=True)
        assert ei.value.kind is CloudBackendErrorKind.UNAVAILABLE
        assert ei.value.retryable
        assert seen == [
            ("MKCOL", "sessions/s-1"),
            ("MKCOL", "sessions"),
            ("MKCOL", "sessions/s-1"),
        ]

    @pytest.mark.asyncio
    async def test_an_existing_leaf_is_still_refused_when_exclusive(self):
        be, seen = self._backend({"sessions", "sessions/s-1"})
        with pytest.raises(CloudBackendError) as ei:
            await be.ensure_session_folder(session_id="s-1", exclusive=True)
        assert ei.value.kind is CloudBackendErrorKind.ALREADY_EXISTS
        assert seen == [("MKCOL", "sessions/s-1")]

    @pytest.mark.asyncio
    async def test_a_session_file_whose_folder_exists_costs_one_put(self):
        be, seen = self._backend({"sessions", "sessions/s-1"})
        await be.put_session_file(
            SessionFolderHandle(backend="nextcloud", native_id="sessions/s-1"),
            path="report.md",
            content=b"r",
        )
        assert seen == [("PUT", "sessions/s-1/report.md")]

    @pytest.mark.asyncio
    async def test_a_session_file_creates_missing_folders_after_the_put(self):
        dirs = {"sessions"}
        be, seen = self._backend(dirs)
        await be.put_session_file(
            SessionFolderHandle(backend="nextcloud", native_id="sessions/s-1"),
            path="out/report.md",
            content=b"r",
        )
        assert seen == [
            ("PUT", "sessions/s-1/out/report.md"),
            ("MKCOL", "sessions"),
            ("MKCOL", "sessions/s-1"),
            ("MKCOL", "sessions/s-1/out"),
            ("PUT", "sessions/s-1/out/report.md"),
        ]


class TestResolveUserIdentity:
    def _backend(self, users: dict) -> tuple[NextcloudBackend, list[str]]:
        searches: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            search = request.url.params["search"]
            searches.append(search)
            found = {
                uid: rec
                for uid, rec in users.items()
                if search.lower() in (uid + " " + str(rec)).lower()
            }
            return httpx.Response(
                200,
                json={"ocs": {"data": {"users": found}}},
                headers={"Content-Type": "application/json"},
            )

        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(handler)
        )
        be._initialized = True
        return be, searches

    @pytest.mark.asyncio
    async def test_an_account_without_an_email_matches_by_display_name(self):
        # Nextcloud refuses an address like admin@localhost, so the account
        # user_oidc creates carries "email": null.
        be, searches = self._backend(
            {
                "oidc-1": {"email": None, "displayname": "test"},
                "other": {"email": None, "displayname": "test runner"},
            }
        )
        assert await be.resolve_user_identity("admin@localhost", "test") == "oidc-1"
        assert searches == ["admin@localhost", "test"]

    @pytest.mark.asyncio
    async def test_a_null_display_name_never_raises(self):
        be, _ = self._backend(
            {
                "a": {"email": "a@example.com", "displayname": None},
                "b": {"email": None, "displayname": None},
            }
        )
        assert await be.resolve_user_identity("a@example.com", "x") == "a"


USER_OIDC_CONFIG = "/ocs/v2.php/apps/provisioning_api/api/v1/config/apps/user_oidc"
USER_OIDC_CREATE = "/ocs/v2.php/apps/user_oidc/api/v1/user"


def _login_uid(provider_id: int, username: str) -> str:
    """The uid user_oidc gives a login under unique uids (LocalIdService::getId)."""
    return hashlib.sha256(f"{provider_id}_0_{username}".encode()).hexdigest()


def _ocs(data: object, *, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status,
        json={"ocs": {"meta": {"statuscode": status}, "data": data}},
    )


class FakeUserOidc:
    """The admin endpoints ``ensure_user`` uses: the user search, user_oidc's
    app config, the user record, and user_oidc's create-user API, which finds
    or creates the account as ``UserMapper::getOrCreate`` does for a login."""

    def __init__(self, config: dict[str, str] | None = None) -> None:
        self.users: dict[str, dict] = {}
        self.config = (
            config
            if config is not None
            else {
                "provider-3-mappingUid": "preferred_username",
                "provider-3-uniqueUid": "1",
                "provider-3-groupProvisioning": "0",
            }
        )
        self.created: list[dict[str, str]] = []
        self.create_status = 200
        self.config_listings = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/ocs/v2.php/cloud/users/details":
            search = request.url.params["search"].lower()
            found = {
                uid: rec
                for uid, rec in self.users.items()
                if search
                in f"{uid} {rec.get('email') or ''} {rec.get('displayname') or ''}".lower()
            }
            return _ocs({"users": found})
        if path == USER_OIDC_CONFIG:
            self.config_listings += 1
            return _ocs({"data": sorted(self.config)})
        if path.startswith(USER_OIDC_CONFIG + "/"):
            return _ocs({"data": self.config.get(path.rsplit("/", 1)[1], "")})
        if path == USER_OIDC_CREATE and request.method == "POST":
            form = dict(parse_qsl(request.content.decode()))
            self.created.append(form)
            if self.create_status != 200:
                return _ocs([], status=self.create_status)
            uid = _login_uid(int(form["providerId"]), form["userId"])
            self.users.setdefault(
                uid,
                {"email": form.get("email"), "displayname": form.get("displayName")},
            )
            return _ocs({"user_id": uid})
        if path.startswith("/ocs/v2.php/cloud/users/") and request.method == "GET":
            uid = path.rsplit("/", 1)[1]
            if uid in self.users:
                return _ocs({"id": uid})
            return _ocs([], status=404)
        raise AssertionError(f"unexpected {request.method} {path}")

    def backend(self) -> NextcloudBackend:
        be = NextcloudBackend(_nc_test_settings())
        be._client = httpx.AsyncClient(
            base_url=NEXTCLOUD_BASE, transport=httpx.MockTransport(self.handler)
        )
        be._initialized = True
        return be


async def _ensure_alice(be: NextcloudBackend, **over: object):
    args: dict[str, object] = dict(
        sub="kc-sub-alice",
        issuer="https://id.example.com/realms/srw",
        email="alice@example.com",
        display_name="Alice",
        preferred_username="alice",
    )
    args.update(over)
    return await be.ensure_user(**args)


class TestEnsureUser:
    """A project member who never signed in to Nextcloud gets the account
    their first SSO login would create, so SRW can put them into the project
    group at once."""

    @pytest.mark.asyncio
    async def test_an_account_the_search_finds_is_returned_and_none_created(self):
        fake = FakeUserOidc()
        fake.users["oidc-1"] = {"email": "alice@example.com", "displayname": "A"}

        assert await _ensure_alice(fake.backend()) == "oidc-1"
        assert fake.created == []

    @pytest.mark.asyncio
    async def test_a_member_without_an_account_gets_the_one_their_login_would(self):
        fake = FakeUserOidc()

        account = await _ensure_alice(fake.backend())

        assert account == _login_uid(3, "alice")
        assert fake.created == [
            {
                "providerId": "3",
                "userId": "alice",
                "displayName": "Alice",
                "email": "alice@example.com",
            }
        ]

    @pytest.mark.asyncio
    async def test_an_account_the_search_misses_is_never_created_again(self):
        """Creating copies Nextcloud's skeleton files into the home again,
        over the user's own copies, so an account that exists under the
        login's uid is returned as it is."""
        fake = FakeUserOidc()
        fake.users[_login_uid(3, "alice")] = {"email": None, "displayname": None}

        assert await _ensure_alice(fake.backend()) == _login_uid(3, "alice")
        assert fake.created == []

    @pytest.mark.asyncio
    async def test_no_account_is_created_without_a_preferred_username(self, caplog):
        fake = FakeUserOidc()

        with caplog.at_level("WARNING"):
            assert await _ensure_alice(fake.backend(), preferred_username=None) is None

        assert fake.created == []
        assert "preferred_username" in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "config",
        [
            {"provider-1-mappingUid": "sub"},
            {},
            {
                "provider-1-mappingUid": "preferred_username",
                "provider-2-mappingUid": "preferred_username",
            },
            {
                "provider-1-mappingUid": "preferred_username",
                "provider-1-uniqueUid": "0",
            },
        ],
        ids=["uid-from-sub", "no-provider", "two-providers", "plain-uids"],
    )
    async def test_no_account_is_created_unless_one_provider_is_the_charts(
        self, config, caplog
    ):
        """Only a provider that takes the uid from preferred_username and
        hashes it, as the chart registers Keycloak, tells SRW which account
        the user's login will find."""
        fake = FakeUserOidc(config)

        with caplog.at_level("WARNING"):
            assert await _ensure_alice(fake.backend()) is None

        assert fake.created == []
        assert caplog.text

    @pytest.mark.asyncio
    async def test_the_provider_is_looked_up_once(self):
        fake = FakeUserOidc()
        be = fake.backend()

        await _ensure_alice(be)
        await _ensure_alice(
            be,
            sub="kc-sub-bob",
            email="bob@example.com",
            display_name="Bob",
            preferred_username="bob",
        )

        assert fake.config_listings == 1
        assert len(fake.created) == 2

    @pytest.mark.asyncio
    async def test_a_refused_create_raises(self):
        fake = FakeUserOidc()
        fake.create_status = 500

        with pytest.raises(CloudBackendError) as exc:
            await _ensure_alice(fake.backend())

        assert exc.value.kind == CloudBackendErrorKind.UNAVAILABLE


class TestSharedClientSendsNoCookies:
    @pytest.mark.asyncio
    async def test_two_requests_carry_no_cookie_header(self, monkeypatch):
        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get("cookie"))
            return httpx.Response(
                200 if request.url.path == "/status.php" else 404,
                json={"instanceid": "oc1234"},
                headers=[
                    ("set-cookie", "cookie_test=test; path=/"),
                    ("set-cookie", "oc_sessionPassphrase=x; path=/; secure"),
                ],
            )

        real_client = httpx.AsyncClient

        def client_with_mock(**kwargs):
            return real_client(**{**kwargs, "transport": httpx.MockTransport(handler)})

        monkeypatch.setattr(httpx, "AsyncClient", client_with_mock)
        be = NextcloudBackend(_nc_test_settings())
        # status.php then the groupfolders probe; the 404 ends init.
        assert await be.ensure_initialized() is False
        assert seen == [None, None]


#: What a stock Nextcloud 33's status.php answers (homelab, 2026-11-17): no
#: ``instanceid`` among the keys.
_STOCK_STATUS = {
    "installed": True,
    "maintenance": False,
    "needsDbUpgrade": False,
    "version": "33.0.0.16",
    "versionstring": "33.0.0",
    "edition": "",
    "productname": "Nextcloud",
    "extendedSupport": False,
}
#: The cookies an unauthenticated GET /index.php/login sets.
_LOGIN_COOKIES = [
    "oc1a2b3c4d5e=8f9a0b1c2d3e4f; path=/; secure; HttpOnly; SameSite=Lax",
    "oc_sessionPassphrase=Zm9v; path=/; secure; HttpOnly; SameSite=Lax",
    "nc_sameSiteCookielax=true; path=/; httponly; secure; samesite=lax",
    "nc_sameSiteCookiestrict=true; path=/; httponly; secure; samesite=strict",
    "__Host-nc_sameSiteCookielax=true; path=/; httponly; secure; samesite=lax",
]


class TestInstallationProofFromSessionCookie:
    """An external Nextcloud proves its installation with the instance id its
    session cookie is named after (external_nextcloud_gives_srw_no_installation_proof)."""

    @staticmethod
    async def _initialize(
        monkeypatch,
        *,
        status: dict,
        login_cookies: list[str],
        login_status: int = 200,
    ) -> tuple[NextcloudBackend, list[httpx.Request]]:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            path = request.url.path
            if path == "/status.php":
                return httpx.Response(200, json=status)
            if path == "/index.php/apps/groupfolders/folders":
                return httpx.Response(
                    200, json={"ocs": {"meta": {"statuscode": 100}, "data": []}}
                )
            if path == "/index.php/login":
                return httpx.Response(
                    login_status,
                    headers=[("set-cookie", c) for c in login_cookies],
                    text="<html>login</html>",
                )
            # WebDAV-root discovery: unprefixed paths.
            return httpx.Response(404)

        real_client = httpx.AsyncClient

        def client_with_mock(**kwargs):
            return real_client(**{**kwargs, "transport": httpx.MockTransport(handler)})

        monkeypatch.setattr(httpx, "AsyncClient", client_with_mock)
        backend = NextcloudBackend(_nc_test_settings())
        assert await backend.ensure_initialized() is True
        return backend, seen

    @pytest.mark.asyncio
    async def test_the_session_cookie_name_sets_the_proof(self, monkeypatch):
        from orchestrator.services.cloud.backend_instance_authority import (
            main_cloud_installation_proof_sha256,
        )

        backend, _ = await self._initialize(
            monkeypatch, status=_STOCK_STATUS, login_cookies=_LOGIN_COOKIES
        )

        assert backend.installation_proof_sha256 == (
            main_cloud_installation_proof_sha256(
                backend_id="nextcloud", remote_identity="oc1a2b3c4d5e"
            )
        )
        await backend.close()

    @pytest.mark.asyncio
    async def test_without_such_a_cookie_the_proof_stays_none(self, monkeypatch):
        cookies = [c for c in _LOGIN_COOKIES if not c.startswith("oc1a2b")]
        backend, _ = await self._initialize(
            monkeypatch, status=_STOCK_STATUS, login_cookies=cookies
        )

        assert backend.installation_proof_sha256 is None
        await backend.close()

    @pytest.mark.asyncio
    async def test_two_instance_shaped_cookies_prove_nothing(self, monkeypatch):
        backend, _ = await self._initialize(
            monkeypatch,
            status=_STOCK_STATUS,
            login_cookies=[*_LOGIN_COOKIES, "ocffffffffff00=x; path=/"],
        )

        assert backend.installation_proof_sha256 is None
        await backend.close()

    @pytest.mark.asyncio
    async def test_a_redirecting_login_page_still_names_the_cookie(self, monkeypatch):
        # user_oidc's auto-redirect answers the login page with a redirect to
        # the identity provider; the session cookie comes with it and the
        # redirect is not followed.
        backend, seen = await self._initialize(
            monkeypatch,
            status=_STOCK_STATUS,
            login_cookies=_LOGIN_COOKIES,
            login_status=303,
        )

        assert backend.installation_proof_sha256 is not None
        assert [r.url.path for r in seen].count("/index.php/login") == 1
        await backend.close()

    @pytest.mark.asyncio
    async def test_the_login_page_is_read_without_credentials(self, monkeypatch):
        backend, seen = await self._initialize(
            monkeypatch, status=_STOCK_STATUS, login_cookies=_LOGIN_COOKIES
        )

        (login,) = [r for r in seen if r.url.path == "/index.php/login"]
        assert login.method == "GET"
        assert "authorization" not in login.headers
        assert "cookie" not in login.headers
        # The admin's other requests still authenticate.
        status = next(r for r in seen if r.url.path == "/status.php")
        assert status.headers["authorization"].startswith("Basic ")
        await backend.close()

    @pytest.mark.asyncio
    async def test_status_php_instanceid_comes_first(self, monkeypatch):
        from orchestrator.services.cloud.backend_instance_authority import (
            main_cloud_installation_proof_sha256,
        )

        backend, seen = await self._initialize(
            monkeypatch,
            status={**_STOCK_STATUS, "instanceid": "ocstatusphp01"},
            login_cookies=_LOGIN_COOKIES,
        )

        assert backend.installation_proof_sha256 == (
            main_cloud_installation_proof_sha256(
                backend_id="nextcloud", remote_identity="ocstatusphp01"
            )
        )
        assert "/index.php/login" not in [r.url.path for r in seen]
        await backend.close()

    @pytest.mark.parametrize(
        ("headers", "expected"),
        [
            (["oc1a2b3c4d5e=v; path=/"], "oc1a2b3c4d5e"),
            # Two Set-Cookie lines for one session (a regenerated id).
            (["oc1a2b3c4d5e=a; path=/", "oc1a2b3c4d5e=b; path=/"], "oc1a2b3c4d5e"),
            (["oc_sessionPassphrase=v", "nc_sameSiteCookielax=true"], None),
            (["oc1a2b=v; path=/"], None),  # too short for an instance id
            (["ocabc_defghijk=v; path=/"], None),
            (["xoc1a2b3c4d5e=v; path=/"], None),
            ([], None),
        ],
    )
    def test_cookie_name_shape(self, headers, expected):
        from orchestrator.services.cloud.nextcloud import (
            instance_id_from_session_cookies,
        )

        assert instance_id_from_session_cookies(headers) == expected


class TestShareRateBucket:
    def test_unset_env_keeps_fifteen_per_ten_minutes(self):
        assert share_rate_bucket({}) == (15, 600.0)

    def test_chart_limit_is_split_between_replicas(self):
        env = {
            "NEXTCLOUD_SHARE_RATE_LIMIT": "200",
            "NEXTCLOUD_SHARE_RATE_PERIOD_SECONDS": "600",
            "NEXTCLOUD_SHARE_RATE_REPLICAS": "2",
        }
        assert share_rate_bucket(env) == (75, 600.0)

    def test_upstream_limit_with_two_replicas_stays_under_the_server(self):
        env = {"NEXTCLOUD_SHARE_RATE_REPLICAS": "2"}
        events, _ = share_rate_bucket(env)
        assert events * 2 <= 20
        assert events == 7

    def test_a_bucket_never_drops_to_zero(self):
        env = {"NEXTCLOUD_SHARE_RATE_LIMIT": "1", "NEXTCLOUD_SHARE_RATE_REPLICAS": "5"}
        assert share_rate_bucket(env)[0] == 1

    @pytest.mark.parametrize("bad", ["0", "-3", "lots", ""])
    def test_invalid_values_fall_back_to_upstream(self, bad):
        env = {
            "NEXTCLOUD_SHARE_RATE_LIMIT": bad,
            "NEXTCLOUD_SHARE_RATE_PERIOD_SECONDS": bad,
            "NEXTCLOUD_SHARE_RATE_REPLICAS": bad,
        }
        assert share_rate_bucket(env) == (15, 600.0)


def _install_fake(backend: NextcloudBackend, fake: FakeNextcloud) -> None:
    """Wire a pre-initialized adapter up to the fake server."""
    backend._client = httpx.AsyncClient(
        base_url=NEXTCLOUD_BASE,
        transport=httpx.MockTransport(fake.handler),
    )
    backend._agent_client = backend._build_agent_client(
        httpx.MockTransport(fake.handler)
    )
    backend._initialized = True
    backend._agent_user = AGENT_USER
    backend._agent_password = "pw"


# --------------------------------------------------------------- list_project_folder


class TestListProjectFolder:
    @pytest.mark.asyncio
    async def test_lists_nested_files(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.txt", b"hello")
        fake.add_file("sub/b.txt", b"world!!")
        fake.add_file("sub/deep/c.txt", b"x")
        _install_fake(be, fake)

        entries = await be.list_project_folder(_handle())

        # Assert on the file-level view the consumer (job_cloud_baseline)
        # actually uses: dirs filtered out, keyed by path.
        files = {e.path: e for e in entries if not e.is_dir}
        assert set(files) == {"a.txt", "sub/b.txt", "sub/deep/c.txt"}
        assert files["a.txt"].size == 5
        assert files["sub/b.txt"].size == 7
        assert files["a.txt"].content_type == "text/plain"
        # Every file carries a captured etag, and each file appears once
        # (recursion found the nested ones without duplicating files).
        assert all(f.etag for f in files.values())
        file_paths = [e.path for e in entries if not e.is_dir]
        assert len(file_paths) == len(set(file_paths))

    @pytest.mark.asyncio
    async def test_does_not_double_count_subdirs(self):
        # Regression for the double-subdir bug (design §11.5): each Depth:1
        # PROPFIND of a subdir returns that subdir's own self-entry, which the
        # walker must drop (its parent already emitted it). Assert directory
        # entries — not just files — appear exactly once.
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("Documents/a.md", b"a")
        fake.add_file("Documents/Sub/b.md", b"b")
        _install_fake(be, fake)

        entries = await be.list_project_folder(_handle())
        paths = [e.path for e in entries]
        assert len(paths) == len(set(paths)), f"duplicate paths: {paths}"
        assert set(paths) == {
            "Documents",
            "Documents/a.md",
            "Documents/Sub",
            "Documents/Sub/b.md",
        }

    @pytest.mark.asyncio
    async def test_etag_is_stable_across_calls(self):
        # External-mod detection compares baseline vs. live etags, so a
        # re-list of an unchanged file must yield the same etag.
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.txt", b"hello")
        _install_fake(be, fake)

        first = {e.path: e.etag for e in await be.list_project_folder(_handle())}
        second = {e.path: e.etag for e in await be.list_project_folder(_handle())}
        assert first["a.txt"] and first == second

    @pytest.mark.asyncio
    async def test_empty_folder_has_no_files(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        entries = await be.list_project_folder(_handle())
        assert [e for e in entries if not e.is_dir] == []


class TestCaptureEtagBaseline:
    @pytest.mark.asyncio
    async def test_prefers_infinity_single_request(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("a.md", b"a")
        fake.add_file("knowledge-base/knowledge/b.md", b"b")
        _install_fake(be, fake)

        base = await be.capture_etag_baseline(_handle())

        assert set(base) == {
            "a.md",
            "knowledge-base/knowledge/b.md",
        }  # files only, no dirs
        assert all(v for v in base.values())  # etags populated
        # Exactly one PROPFIND — the infinity short-circuit, not a per-dir BFS.
        propfinds = [r for r in fake.requests if r.method == "PROPFIND"]
        assert len(propfinds) == 1
        assert propfinds[0].headers.get("Depth") == "infinity"

    @pytest.mark.asyncio
    async def test_falls_back_to_bfs_when_infinity_rejected(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.reject_infinity = True  # sabre with infinity disabled → 400
        fake.add_file("a.md", b"a")
        fake.add_file("knowledge-base/knowledge/b.md", b"b")
        _install_fake(be, fake)

        base = await be.capture_etag_baseline(_handle())

        assert set(base) == {
            "a.md",
            "knowledge-base/knowledge/b.md",
        }  # same result via BFS
        methods = [r for r in fake.requests if r.method == "PROPFIND"]
        # One rejected infinity attempt + at least the root + docs Depth:1 walks.
        assert any(r.headers.get("Depth") == "infinity" for r in methods)
        assert any(r.headers.get("Depth") == "1" for r in methods)

    @pytest.mark.asyncio
    async def test_missing_mountpoint_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.list_project_folder(_handle(mountpoint=None))
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- get_project_folder_file_bytes


class TestGetBytes:
    @pytest.mark.asyncio
    async def test_returns_bytes(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("knowledge-base/knowledge/readme.md", b"# hi\n")
        _install_fake(be, fake)
        blob = await be.get_project_folder_file_bytes(
            _handle(), path="knowledge-base/knowledge/readme.md"
        )
        assert blob == b"# hi\n"

    @pytest.mark.asyncio
    async def test_binary_survives(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        payload = bytes(range(256))
        fake.add_file("blob.bin", payload)
        _install_fake(be, fake)
        assert (
            await be.get_project_folder_file_bytes(_handle(), path="blob.bin")
            == payload
        )

    @pytest.mark.asyncio
    async def test_missing_raises_not_found(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.get_project_folder_file_bytes(_handle(), path="nope.txt")
        assert ei.value.kind == CloudBackendErrorKind.NOT_FOUND

    @pytest.mark.asyncio
    async def test_empty_path_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.get_project_folder_file_bytes(_handle(), path="")
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- put_project_folder_file_bytes


class TestPutBytes:
    @pytest.mark.asyncio
    async def test_creates_parents_and_round_trips(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(
            _handle(), path="x/y/z.txt", content=b"data", content_type="text/plain"
        )
        # Each parent collection was MKCOL'd on the way (in order).
        mkcols = [unquote(r.url.path) for r in fake.requests if r.method == "MKCOL"]
        assert any(p.endswith("/x") for p in mkcols)
        assert any(p.endswith("/x/y") for p in mkcols)
        # File is stored and reads back identically.
        assert (
            await be.get_project_folder_file_bytes(_handle(), path="x/y/z.txt")
            == b"data"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("missing_status", [404, 409])
    async def test_a_missing_parent_is_created_after_the_put_is_refused(
        self, missing_status
    ):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.missing_parent_put_status = missing_status
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(
            _handle(), path="x/y/z.txt", content=b"d"
        )
        assert [
            (r.method, unquote(r.url.path)[len(fake.decoded_base) :])
            for r in fake.requests
        ] == [
            ("PUT", "/x/y/z.txt"),
            ("MKCOL", "/x"),
            ("MKCOL", "/x/y"),
            ("PUT", "/x/y/z.txt"),
        ]
        assert fake.files["x/y/z.txt"] == b"d"

    @pytest.mark.asyncio
    async def test_a_file_whose_folders_exist_costs_one_put(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("x/y/old.txt", b"o")
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(
            _handle(), path="x/y/z.txt", content=b"d"
        )
        assert [r.method for r in fake.requests] == ["PUT"]

    @pytest.mark.asyncio
    async def test_a_put_refused_with_its_parents_there_is_not_retried_twice(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("x/y/old.txt", b"o")
        fake.files["x/y/z.txt"] = b"locked"
        _install_fake(be, fake)
        original_put = fake._put
        fake._put = lambda rel, content: (
            httpx.Response(409) if rel == "x/y/z.txt" else original_put(rel, content)
        )
        with pytest.raises(CloudBackendError):
            await be.put_project_folder_file_bytes(
                _handle(), path="x/y/z.txt", content=b"d"
            )
        assert [r.method for r in fake.requests] == ["PUT", "MKCOL", "MKCOL", "PUT"]

    @pytest.mark.asyncio
    async def test_root_file_needs_no_parents(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        _install_fake(be, fake)
        await be.put_project_folder_file_bytes(_handle(), path="top.txt", content=b"t")
        assert fake.files["top.txt"] == b"t"
        assert [r for r in fake.requests if r.method == "MKCOL"] == []

    @pytest.mark.asyncio
    async def test_empty_path_raises_invalid_request(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.put_project_folder_file_bytes(_handle(), path="", content=b"x")
        assert ei.value.kind == CloudBackendErrorKind.INVALID_REQUEST


# ----------------------------------------------------- delete_project_folder_file


class TestDeleteFile:
    @pytest.mark.asyncio
    async def test_removes_file(self):
        be = NextcloudBackend(_nc_test_settings())
        fake = FakeNextcloud()
        fake.add_file("gone.txt", b"x")
        _install_fake(be, fake)
        await be.delete_project_folder_file(_handle(), path="gone.txt")
        assert "gone.txt" not in fake.files

    @pytest.mark.asyncio
    async def test_missing_with_if_exists_true_is_noop(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        # No raise — the goal state ("gone") already holds.
        await be.delete_project_folder_file(_handle(), path="nope.txt")

    @pytest.mark.asyncio
    async def test_missing_with_if_exists_false_raises_not_found(self):
        be = NextcloudBackend(_nc_test_settings())
        _install_fake(be, FakeNextcloud())
        with pytest.raises(CloudBackendError) as ei:
            await be.delete_project_folder_file(
                _handle(), path="nope.txt", if_exists=False
            )
        assert ei.value.kind == CloudBackendErrorKind.NOT_FOUND


# ------------------------------------------------------------------- uninitialized


class TestUninitialized:
    @pytest.mark.asyncio
    async def test_list_uninitialized_raises_unavailable(self):
        be = NextcloudBackend(_nc_test_settings())  # never wired to a client
        with pytest.raises(CloudBackendError) as ei:
            await be.list_project_folder(_handle())
        assert ei.value.kind == CloudBackendErrorKind.UNAVAILABLE


class TestRcloneMountSpec:
    """rclone's nextcloud vendor builds chunked uploads from a
    /dav/files/<user>/ URL and refuses any other ("the remote url looks
    incorrect"): a Group Folder's /dav/groupfolders/ URL mounts with
    whole-file uploads, every other surface keeps chunking."""

    @pytest.mark.asyncio
    async def test_a_group_folder_mounts_without_chunked_uploads(self):
        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
            subject=CloudMountSubject(username=_WRITER, credential="writer-pw"),
        )
        assert spec.source_config["url"].startswith(
            f"{NEXTCLOUD_BASE}/remote.php/dav/groupfolders/{_WRITER}/"
        )
        assert spec.source_config["user"] == _WRITER
        assert spec.auth == {"type": "basic", "password": "writer-pw"}
        assert spec.source_config["nextcloud_chunk_size"] == "0"
        assert spec.source_config["vendor"] == "nextcloud"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "subject",
        [
            None,
            CloudMountSubject(),
            CloudMountSubject(username=AGENT_USER),
            CloudMountSubject(username=_WRITER),
        ],
    )
    async def test_a_project_folder_is_never_mounted_as_the_service_account(
        self, subject
    ):
        """Main cloud slice 7: no per-folder grant, no project mount."""
        be = NextcloudBackend(_nc_test_settings())
        with pytest.raises(CloudBackendError) as caught:
            await be.build_rclone_mount_spec(
                handle=_handle(),
                mount_kind="project",
                target_path="/cloud/test",
                access="read_write",
                subject=subject,
            )
        assert caught.value.kind == CloudBackendErrorKind.NOT_SUPPORTED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("username", "access"),
        [
            (AGENT_USER, "read_write"),
            ("admin", "read_only"),
            (_READER, "read_write"),
            # Read-only is the reader's doing, never a writer behind a flag.
            (_WRITER, "read_only"),
            (_WRITER, "protected"),
            (_READER, "protected"),
        ],
    )
    async def test_only_a_grant_principal_at_its_level_is_mounted(
        self, username, access
    ):
        be = NextcloudBackend(_nc_test_settings())
        with pytest.raises(CloudBackendError) as caught:
            await be.build_rclone_mount_spec(
                handle=_handle(),
                mount_kind="project",
                target_path="/cloud/test",
                access=access,
                subject=CloudMountSubject(username=username, credential="pw"),
            )
        assert caught.value.kind == CloudBackendErrorKind.INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_a_reader_mounts_read_only_over_the_public_url_on_a_vm(self):
        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_only",
            subject=CloudMountSubject(username=_READER, credential="reader-pw"),
            prefer_public_url=True,
        )
        assert spec.source_config["user"] == _READER
        assert spec.source_config["url"].startswith(
            f"{be._public_url}/remote.php/dav/groupfolders/{_READER}/"
        )

    @pytest.mark.asyncio
    async def test_ends_reconstruction_of_an_old_mount_keeps_its_bytes(self):
        """The old rule's in-workspace mount, rebuilt for End to adopt and
        remove its resident process: the service account, as it was."""
        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=_handle(),
            mount_kind="project",
            target_path="/cloud/test",
            access="read_write",
            subject=CloudMountSubject(legacy_service_account=True),
        )
        assert spec.source_config["user"] == AGENT_USER
        assert spec.source_config["url"].startswith(
            f"{NEXTCLOUD_BASE}/remote.php/dav/groupfolders/{AGENT_USER}/"
        )

    @staticmethod
    def _home(uid: str = "a" * 64):
        return ProjectFolderHandle(
            backend="nextcloud",
            native_id=f"home:{uid}",
            vendor_meta={"kind": "user_home", "username": uid},
        )

    @pytest.mark.asyncio
    async def test_a_user_root_mounts_as_its_user_with_chunked_uploads(self):
        """Main cloud slice 4: the user's own account, its own app password
        (as the login name it was issued for), the account's /dav/files/."""
        be = NextcloudBackend(_nc_test_settings())
        uid = "a" * 64
        spec = await be.build_rclone_mount_spec(
            handle=self._home(uid),
            mount_kind="user_root",
            target_path="/cloud/my_cloud",
            access="read_write",
            subject=CloudMountSubject(username="ann", credential="app-pw"),
        )
        assert spec.source_config == {
            "url": f"{NEXTCLOUD_BASE}/remote.php/dav/files/{uid}/",
            "vendor": "nextcloud",
            "user": "ann",
        }
        assert spec.auth == {"type": "basic", "password": "app-pw"}
        assert "nextcloud_chunk_size" not in spec.source_config

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "subject",
        [
            None,
            CloudMountSubject(username="ann"),
            CloudMountSubject(credential="app-pw"),
            CloudMountSubject(legacy_service_account=True),
        ],
    )
    async def test_a_user_root_without_its_users_credential_never_mounts(
        self, subject, monkeypatch
    ):
        # The old env escape hatch is gone: nothing but the user's own
        # credential mounts their root.
        monkeypatch.setenv("NEXTCLOUD_RCLONE_USER_HOME_USERNAME", "a" * 64)
        monkeypatch.setenv("NEXTCLOUD_RCLONE_USER_HOME_PASSWORD", "env-pw")
        be = NextcloudBackend(_nc_test_settings())
        with pytest.raises(CloudBackendError) as caught:
            await be.build_rclone_mount_spec(
                handle=self._home(),
                mount_kind="user_root",
                target_path="/cloud/my_cloud",
                access="read_write",
                subject=subject,
            )
        assert caught.value.kind == CloudBackendErrorKind.NOT_SUPPORTED

    def test_it_grants_user_roots(self):
        from orchestrator.services.cloud.user_root_grant import (
            SupportsUserRootGrant,
        )

        be = NextcloudBackend(_nc_test_settings())
        assert isinstance(be, SupportsUserRootGrant)
        assert be.capabilities.offers(
            "cloud_folder", "user_root", "read_write", workspace_backend="sandbox"
        )
        assert not be.capabilities.offers(
            "cloud_folder", "user_root", "read_write", workspace_backend="vm"
        )

    @pytest.mark.asyncio
    async def test_a_session_folder_keeps_chunked_uploads(self):
        from orchestrator.services.cloud import SessionFolderHandle

        be = NextcloudBackend(_nc_test_settings())
        spec = await be.build_rclone_mount_spec(
            handle=SessionFolderHandle(backend="nextcloud", native_id="sessions/ab"),
            mount_kind="session_folder",
            target_path="/cloud/home",
            access="read_write",
        )
        assert "/remote.php/dav/files/" in spec.source_config["url"]
        assert "nextcloud_chunk_size" not in spec.source_config
