"""Nextcloud's half of the user-root grant (main cloud slice 4).

``NextcloudBackend`` mixes :class:`NextcloudUserRootGrant` in to implement
``SupportsUserRootGrant``: a user grants SRW an app password for their own
account through Login Flow v2 ("Connect your cloud"). The flow is started
and polled on the internal URL; the page the user opens is the public one.
Nextcloud names the app password after the client that started the flow
(the ``User-Agent`` of the init request), so the user finds it as
"Superhuman Remote Worker" under Settings, Security, Devices & sessions,
and revoking it there is honoured at SRW's next probe.

Nextcloud accepts an app password only with the login name it was issued
for, so every call made with the credential authenticates as that login
name; the account's files live under its user id (``/dav/files/<uid>/``),
which the credential itself names (``/ocs/v2.php/cloud/user``).

Every request made with the user's credential goes through a client of its
own, with no cookie from the administrator's session, so Nextcloud can only
authenticate it as the credential's account. Nothing here logs, raises or
returns the credential.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
(slice 4) and nextcloud_user_home_app_token_grant.md.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.handles import ProjectFolderHandle
from orchestrator.services.cloud.user_root_grant import (
    GRANT_MODES,
    CloudAccount,
    LoginFlowGrant,
    LoginFlowStart,
    UserRootGrantMode,
    VerifyOutcome,
)

logger = logging.getLogger(__name__)

#: The deployment's grant mode (``login_flow``, the default, or ``off``).
GRANT_MODE_ENV = "NEXTCLOUD_USER_ROOT_GRANT_MODE"
#: The name the app password carries in the user's device list.
CLIENT_NAME = "Superhuman Remote Worker"
#: The Nextcloud group whose members administer the instance.
_ADMIN_GROUP = "admin"
#: What SRW's own accounts and groups are named with: ``srw-agents`` (the
#: agent service account's group) and every principal and group SRW
#: creates (``srw-reader-f-…``, ``srw-rog-a-…``).
_SERVICE_PREFIX = "srw-"

_LOGIN_FLOW_INIT = "/index.php/login/v2"
_LOGIN_FLOW_POLL = "/index.php/login/v2/poll"
_CURRENT_USER = "/ocs/v2.php/cloud/user"
_APP_PASSWORD = "/ocs/v2.php/core/apppassword"
_APP_PASSWORD_ROTATE = "/ocs/v2.php/core/apppassword/rotate"

_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)


def _root_path(account_id: str) -> str:
    return f"/remote.php/dav/files/{quote(str(account_id), safe='')}/"


class NextcloudUserRootGrant:
    """``SupportsUserRootGrant`` for ``NextcloudBackend``.

    Reads ``_base_url``, ``_public_url``, ``_client`` (the administrator's),
    ``_agent_user`` (when it has one) and ``_ensure_ready`` from the backend
    it is mixed into.
    """

    backend_id: str
    _base_url: str
    _public_url: str
    _client: Optional[httpx.AsyncClient]

    #: The transport of the per-call clients (a test's mock; ``None`` is
    #: httpx's own).
    _user_root_transport: Optional[httpx.AsyncBaseTransport] = None

    @property
    def user_root_grant_mode(self) -> UserRootGrantMode:
        """``login_flow`` unless the deployment turned it ``off``.

        ``silent`` is reserved for minting without a click, which is not
        built: it, like any other value, is refused as ``off``.
        """
        raw = (os.getenv(GRANT_MODE_ENV) or "login_flow").strip().lower()
        if raw in GRANT_MODES:
            return raw  # type: ignore[return-value]
        logger.warning(
            "%s=%r is not a grant mode SRW offers (%s); personal cloud roots are off",
            GRANT_MODE_ENV,
            raw,
            ", ".join(GRANT_MODES),
        )
        return "off"

    @property
    def user_root_client_name(self) -> str:
        return CLIENT_NAME

    def _account_client(
        self, auth: tuple[str, str] | None = None, *, ocs: bool = True
    ) -> httpx.AsyncClient:
        """A fresh client: no cookie of another request, no redirects."""
        headers = {"User-Agent": CLIENT_NAME}
        if ocs:
            headers.update({"OCS-APIRequest": "true", "Accept": "application/json"})
        return httpx.AsyncClient(
            base_url=self._base_url,
            auth=auth,
            headers=headers,
            timeout=_TIMEOUT,
            follow_redirects=False,
            transport=self._user_root_transport,
        )

    def _error(
        self,
        kind: CloudBackendErrorKind,
        message: str,
        *,
        status_code: int | None = None,
    ) -> CloudBackendError:
        # Never ``raw``: these answers may carry a credential.
        return CloudBackendError(
            kind,
            message,
            backend=self.backend_id,
            status_code=status_code,
            retryable=kind
            in (CloudBackendErrorKind.UNAVAILABLE, CloudBackendErrorKind.TIMEOUT),
        )

    def _status_error(self, what: str, status: int) -> CloudBackendError:
        kind = (
            CloudBackendErrorKind.UNAVAILABLE
            if status >= 500 or status == 429
            else CloudBackendErrorKind.UNKNOWN
        )
        return self._error(kind, f"{what}: HTTP {status}", status_code=status)

    def _transport_error(self, what: str, exc: httpx.HTTPError) -> CloudBackendError:
        kind = (
            CloudBackendErrorKind.TIMEOUT
            if isinstance(exc, httpx.TimeoutException)
            else CloudBackendErrorKind.UNAVAILABLE
        )
        return self._error(kind, f"{what}: {type(exc).__name__}")

    def _public_page_url(self, url: str) -> str:
        """``url`` on the public host when Nextcloud built it for the
        internal one (it answers with the host it was asked on)."""
        page, base, public = (
            urlsplit(url),
            urlsplit(self._base_url),
            urlsplit(self._public_url),
        )
        if not public.netloc or page.netloc != base.netloc:
            return url
        path = page.path
        base_path = base.path.rstrip("/")
        if base_path and path.startswith(base_path):
            path = path[len(base_path) :]
        path = public.path.rstrip("/") + path
        return urlunsplit((public.scheme, public.netloc, path, page.query, ""))

    def _on_public_host(self, url: str) -> bool:
        """Whether ``url`` is an http(s) page of the main cloud's public
        host (same host and port): the only page SRW sends a user to."""
        try:
            page, public = urlsplit(url), urlsplit(self._public_url or self._base_url)
            page_port, public_port = page.port, public.port
        except ValueError:
            return False
        defaults = {"http": 80, "https": 443}
        return (
            page.scheme in defaults
            and public.scheme in defaults
            and bool(page.hostname)
            and page.hostname == public.hostname
            and (page_port or defaults[page.scheme])
            == (public_port or defaults[public.scheme])
        )

    @staticmethod
    def _ocs_data(resp: httpx.Response) -> dict[str, Any]:
        try:
            body = resp.json()
        except ValueError:
            return {}
        data = (body.get("ocs") or {}).get("data") if isinstance(body, dict) else None
        return data if isinstance(data, dict) else {}

    # ---------------------------------------------------------- Login Flow v2

    async def start_login_flow(self) -> LoginFlowStart:
        self._ensure_ready()
        try:
            async with self._account_client(ocs=False) as client:
                resp = await client.post(_LOGIN_FLOW_INIT)
        except httpx.HTTPError as exc:
            raise self._transport_error("login flow start", exc) from None
        if resp.status_code != 200:
            raise self._status_error("login flow start", resp.status_code)
        try:
            body = resp.json()
            poll_token = str(body["poll"]["token"])
            login_url = str(body["login"])
        except (ValueError, KeyError, TypeError):
            raise self._error(
                CloudBackendErrorKind.UNKNOWN, "login flow start: unexpected answer"
            ) from None
        if not poll_token or not login_url:
            raise self._error(
                CloudBackendErrorKind.UNKNOWN, "login flow start: unexpected answer"
            )
        login_url = self._public_page_url(login_url)
        if not self._on_public_host(login_url):
            # The page a user signs in on: never another host or scheme,
            # whatever the cloud (or something in front of it) answered.
            raise self._error(
                CloudBackendErrorKind.UNKNOWN,
                "login flow start: the sign-in page is not on the cloud's public host",
            )
        return LoginFlowStart(login_url=login_url, poll_token=poll_token)

    async def poll_login_flow(self, poll_token: str) -> Optional[LoginFlowGrant]:
        """Polled on the internal URL, never the ``poll.endpoint`` Nextcloud
        names (its public URL, which would hairpin through the edge)."""
        self._ensure_ready()
        try:
            async with self._account_client(ocs=False) as client:
                resp = await client.post(_LOGIN_FLOW_POLL, data={"token": poll_token})
        except httpx.HTTPError as exc:
            raise self._transport_error("login flow poll", exc) from None
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise self._status_error("login flow poll", resp.status_code)
        try:
            body = resp.json()
            login_name = body.get("loginName")
            credential = body.get("appPassword")
        except (ValueError, AttributeError):
            login_name = credential = None
        if not login_name or not credential:
            raise self._error(
                CloudBackendErrorKind.UNKNOWN, "login flow poll: incomplete answer"
            )
        return LoginFlowGrant(login_name=str(login_name), credential=str(credential))

    # ------------------------------------------------------------- Accounts

    async def user_root_account_id(
        self, login_name: str, credential: str
    ) -> Optional[str]:
        try:
            async with self._account_client((login_name, credential)) as client:
                resp = await client.get(_CURRENT_USER, params={"format": "json"})
        except httpx.HTTPError as exc:
            raise self._transport_error("current user", exc) from None
        if resp.status_code in (401, 403):
            return None
        if resp.status_code != 200:
            raise self._status_error("current user", resp.status_code)
        account_id = self._ocs_data(resp).get("id")
        if not account_id:
            raise self._error(
                CloudBackendErrorKind.UNKNOWN, "current user: no account id"
            )
        return str(account_id)

    async def user_root_account(self, account_id: str) -> Optional[CloudAccount]:
        self._ensure_ready()
        assert self._client is not None  # guarded by _ensure_ready
        try:
            resp = await self._client.get(
                f"/ocs/v2.php/cloud/users/{quote(str(account_id), safe='')}",
                params={"format": "json"},
            )
        except httpx.HTTPError as exc:
            raise self._transport_error("account", exc) from None
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise self._status_error("account", resp.status_code)
        data = self._ocs_data(resp)
        if not data.get("id"):
            return None
        account = str(data["id"])
        groups = data.get("groups")
        groups = [str(group) for group in groups] if isinstance(groups, list) else []
        # The groups the account is a group admin of (Nextcloud lists their
        # ids); any one makes it an administrator of other accounts.
        subadmin = data.get("subadmin")
        email = data.get("email")
        agent_user = str(getattr(self, "_agent_user", "") or "")
        return CloudAccount(
            account_id=account,
            email=str(email) if email else None,
            enabled=data.get("enabled") is not False,
            administrator=_ADMIN_GROUP in groups or bool(subadmin),
            service_account=(
                account.casefold().startswith(_SERVICE_PREFIX)
                or (bool(agent_user) and account.casefold() == agent_user.casefold())
                or any(group.casefold().startswith(_SERVICE_PREFIX) for group in groups)
            ),
        )

    def user_root_handle(self, account_id: str) -> ProjectFolderHandle:
        """The same handle ``get_user_home`` builds: the account's root,
        mounted from ``/dav/files/<account id>/``."""
        return ProjectFolderHandle(
            backend=self.backend_id,
            native_id=f"home:{account_id}",
            vendor_meta={"kind": "user_home", "username": str(account_id)},
        )

    # ------------------------------------------------- Verify, revoke, rotate

    async def verify_user_root(
        self, account_id: str, login_name: str, credential: str
    ) -> VerifyOutcome:
        """A ``Depth: 0`` PROPFIND of the account's root: 207 reads it, 401
        or 403 is a refused credential, 404 an account without files."""
        try:
            async with self._account_client(
                (login_name, credential), ocs=False
            ) as client:
                resp = await client.request(
                    "PROPFIND", _root_path(account_id), headers={"Depth": "0"}
                )
        except httpx.HTTPError:
            return "unreachable"
        if resp.status_code == 207:
            return "ok"
        if resp.status_code in (401, 403):
            return "rejected"
        if resp.status_code == 404:
            return "missing"
        return "unreachable"

    async def revoke_user_root(self, login_name: str, credential: str) -> bool:
        """The documented self-revocation: ``DELETE /ocs/v2.php/core/
        apppassword`` authenticated with the app password itself. A 401
        means Nextcloud no longer accepts it, which is what a revoke wants."""
        try:
            async with self._account_client((login_name, credential)) as client:
                resp = await client.delete(_APP_PASSWORD, params={"format": "json"})
        except httpx.HTTPError as exc:
            logger.warning("App password revoke failed: %s", type(exc).__name__)
            return False
        if resp.status_code in (200, 401):
            return True
        logger.warning("App password revoke answered HTTP %s", resp.status_code)
        return False

    async def rotate_user_root(self, login_name: str, credential: str) -> str:
        try:
            async with self._account_client((login_name, credential)) as client:
                resp = await client.post(
                    _APP_PASSWORD_ROTATE, params={"format": "json"}
                )
        except httpx.HTTPError as exc:
            raise self._transport_error("app password rotate", exc) from None
        if resp.status_code != 200:
            raise self._status_error("app password rotate", resp.status_code)
        rotated = self._ocs_data(resp).get("apppassword")
        if not rotated:
            raise self._error(
                CloudBackendErrorKind.UNKNOWN, "app password rotate: no new password"
            )
        return str(rotated)


__all__ = ["CLIENT_NAME", "GRANT_MODE_ENV", "NextcloudUserRootGrant"]
