"""agent-service's app password (bundled Nextcloud performance, WP1b).

``agent-service`` makes most of SRW's Nextcloud calls: session folders and
their shares, project-folder DAV, and every agent push and session-folder
mount. Signed in with its login password, each request pays a full Argon2
login. Signed in with an app password, Nextcloud looks the token up (cached
for ten seconds) and re-checks the password it stores at most every five
minutes.

The adapter mints one app password per installation and stores it encrypted
in ``main_cloud_service_credentials`` (``service_credentials``), so every
replica and every restart uses the same one:

* **Mint.** ``GET /ocs/v2.php/core/getapppassword`` as agent-service with its
  *login* password, on a fresh client that sends no cookie. Nextcloud 31
  refuses the call when the request itself authenticated with an app
  password, stores the login password in the token (so its periodic re-check
  passes), and names the token after the request's User-Agent:
  :func:`app_password_name`, one deterministic name per installation.
* **Adopt**, lazily before the first agent-service request (or earlier,
  through :meth:`~NextcloudAgentServiceCredential.ensure_service_credentials`):
  the stored token, else a minted one inserted first-writer-wins. A replica
  that loses the insert uses the winner's and revokes its own.
* **Refresh** on a 401: a token another replica stored meanwhile is adopted;
  otherwise a new one is minted and swapped in compare-and-set. The replaced
  token is not revoked: a password login (the mint is one) re-encrypts the
  new login password into every token of the account, which revives a token
  the password change had invalidated, and agents still mounted with it keep
  working until their next payload.
* **Fallback.** Any mint or store failure leaves agent-service on its login
  password (cookie-less, as before). A degraded cloud beats no cloud.

Nothing here logs, raises or returns a credential: log lines carry status
codes and exception type names only.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections.abc import AsyncGenerator, Generator
from typing import Any, Literal, Optional

import httpx

from orchestrator.security import crypto
from orchestrator.services.cloud.service_credentials import (
    ServiceCredentialStore,
    service_credential_store,
)
from shared.http_no_cookies import NoCookieJar

logger = logging.getLogger(__name__)

#: The User-Agent of agent-service's own requests. Every identity has its
#: own, so Nextcloud's logs and token rows can be attributed.
AGENT_CLIENT_USER_AGENT = "SuperhumanRemoteWorker/0.1 agent-service"
#: How long after a failed adoption the next request tries again.
ADOPT_RETRY_SECONDS = 180.0
#: The least time between two app passwords one adapter mints.
MINT_INTERVAL_SECONDS = 60.0

_GET_APP_PASSWORD = "/ocs/v2.php/core/getapppassword"
_APP_PASSWORD = "/ocs/v2.php/core/apppassword"
_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)

AgentSecretKind = Literal["password", "app_password"]


def app_password_name(backend_instance_id: str) -> str:
    """The name Nextcloud lists agent-service's app password under (the
    User-Agent of the minting request), one per installation."""
    return f"SuperhumanRemoteWorker agent-service {backend_instance_id}"


def _basic_authorization(username: str, secret: str) -> str:
    raw = f"{username}:{secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


class _AgentSecretUnavailable(Exception):
    """Why no app password could be had, in words that carry no secret."""


class AgentServiceAuth(httpx.Auth):
    """``Authorization: Basic`` with agent-service's current secret.

    A 401 asks the adapter for a newer secret (passing the generation the
    request was sent with); when one is in place the request, body included,
    is sent once more. A second 401 is the caller's.
    """

    requires_request_body = True

    def __init__(self, credential: NextcloudAgentServiceCredential) -> None:
        self._credential = credential

    def sync_auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        raise RuntimeError("agent-service authentication is asynchronous only")

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        generation = await self._credential._authorize_agent_request(request)
        response = yield request
        if response.status_code == 401 and (
            await self._credential._refresh_agent_secret(generation)
        ):
            self._credential._set_agent_authorization(request)
            yield request


class NextcloudAgentServiceCredential:
    """agent-service's secret for ``NextcloudBackend``: its login password
    until an app password is adopted, then the app password.

    Reads ``_base_url``, ``_agent_user``, ``_agent_password`` (the login
    password) and ``_backend_instance_id`` from the backend it is mixed
    into, which calls :meth:`_init_agent_secret` from its constructor.
    """

    _base_url: str
    _agent_user: str
    _agent_password: str
    _backend_instance_id: Optional[str]

    #: The transport of the mint and revoke clients (a test's mock; ``None``
    #: is httpx's own).
    _agent_secret_transport: Optional[httpx.AsyncBaseTransport] = None

    def _init_agent_secret(self) -> None:
        self._agent_app_password: Optional[str] = None
        #: Moves on every change of secret: a 401 on an older one only
        #: re-sends the request.
        self._agent_secret_generation = 0
        #: The stored row's revision of the adopted app password.
        self._agent_secret_revision: Optional[int] = None
        #: A stored revision Nextcloud refused: replaced, never re-adopted.
        self._agent_refused_revision: Optional[int] = None
        self._agent_secret_lock = asyncio.Lock()
        self._agent_adopt_not_before = 0.0
        self._agent_mint_not_before = 0.0
        self._agent_secret_failure_logged = False

    # ------------------------------------------------------------ The secret

    @property
    def _agent_secret_kind(self) -> AgentSecretKind:
        return "password" if self._agent_app_password is None else "app_password"

    @property
    def _agent_secret(self) -> str:
        if self._agent_app_password is not None:
            return self._agent_app_password
        return self._agent_password

    def agent_service_auth(self) -> AgentServiceAuth:
        """The ``auth=`` of the adapter's agent-service client."""
        return AgentServiceAuth(self)

    def _set_agent_authorization(self, request: httpx.Request) -> int:
        request.headers["Authorization"] = _basic_authorization(
            self._agent_user, self._agent_secret
        )
        return self._agent_secret_generation

    async def _authorize_agent_request(self, request: httpx.Request) -> int:
        if self._agent_adoption_due():
            await self.ensure_service_credentials()
        return self._set_agent_authorization(request)

    def _use_agent_app_password(self, secret: str, revision: int) -> None:
        self._agent_app_password = secret
        self._agent_secret_revision = revision
        self._agent_secret_generation += 1
        self._agent_secret_failure_logged = False

    def _use_agent_login_password(self) -> None:
        self._agent_app_password = None
        self._agent_secret_revision = None
        self._agent_secret_generation += 1

    # --------------------------------------------------- Adoption and refresh

    def _agent_adoption_due(self) -> bool:
        return (
            self._agent_app_password is None
            and self._backend_instance_id is not None
            and service_credential_store() is not None
            and time.monotonic() >= self._agent_adopt_not_before
        )

    async def ensure_service_credentials(self) -> None:
        """Adopt agent-service's app password now: the stored one, or one
        minted and stored.

        A no-op once adopted, before the adapter is bound to its
        installation, without a credential store, and for a while after an
        attempt that failed; agent-service keeps its login password
        meanwhile. Never raises.
        """
        if not self._agent_adoption_due():
            return
        async with self._agent_secret_lock:
            store = service_credential_store()
            instance_id = self._backend_instance_id
            if not self._agent_adoption_due() or store is None or instance_id is None:
                return
            if not await self._obtain_agent_app_password_or_log(
                store, instance_id, replace_revision=self._agent_refused_revision
            ):
                self._agent_adopt_not_before = time.monotonic() + ADOPT_RETRY_SECONDS

    async def _refresh_agent_secret(self, generation: int) -> bool:
        """After a 401 on ``generation``: whether another secret is in place
        to send the request once more with.

        A newer one is there already when the generation moved. On the login
        password there is nothing to refresh. A refused app password is
        replaced by the one another replica stored, else by a new one; when
        neither can be had, agent-service falls back to its login password
        until the next adoption attempt.
        """
        async with self._agent_secret_lock:
            if generation != self._agent_secret_generation:
                return True
            if self._agent_app_password is None:
                return False
            self._agent_refused_revision = self._agent_secret_revision
            store = service_credential_store()
            instance_id = self._backend_instance_id
            if (
                store is None
                or instance_id is None
                or not await self._obtain_agent_app_password_or_log(
                    store, instance_id, replace_revision=self._agent_refused_revision
                )
            ):
                self._use_agent_login_password()
                self._agent_adopt_not_before = time.monotonic() + ADOPT_RETRY_SECONDS
            return True

    async def _obtain_agent_app_password_or_log(
        self,
        store: ServiceCredentialStore,
        instance_id: str,
        *,
        replace_revision: Optional[int],
    ) -> bool:
        try:
            return await self._obtain_agent_app_password(
                store, instance_id, replace_revision=replace_revision
            )
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, _AgentSecretUnavailable)
                else type(exc).__name__
            )
            level = (
                logging.DEBUG if self._agent_secret_failure_logged else logging.WARNING
            )
            self._agent_secret_failure_logged = True
            logger.log(
                level,
                "Nextcloud agent-service has no app password on installation %s "
                "(%s); it signs in with its login password",
                instance_id,
                reason,
            )
            return False

    async def _obtain_agent_app_password(
        self,
        store: ServiceCredentialStore,
        instance_id: str,
        *,
        replace_revision: Optional[int],
    ) -> bool:
        """Adopt the stored app password, or mint one and store it.

        A row at ``replace_revision`` (an app password Nextcloud refused) or
        one that no longer decrypts is replaced compare-and-set at its
        revision. Raises when no app password can be had.
        """
        login = self._agent_user
        row = await store.get_main_cloud_service_credential(instance_id, login)
        expected: Optional[int] = None
        if row is not None:
            revision = int(row["revision"])
            secret = _decrypt(row)
            if secret is not None and revision != replace_revision:
                self._use_agent_app_password(secret, revision)
                return True
            expected = revision
        # Before minting: a token nobody can store would only be revoked.
        crypto.get_cipher()
        now = time.monotonic()
        if now < self._agent_mint_not_before:
            raise _AgentSecretUnavailable(
                "an app password was minted less than a minute ago"
            )
        self._agent_mint_not_before = now + MINT_INTERVAL_SECONDS
        minted = await self._mint_agent_app_password(instance_id)
        try:
            stored = await store.put_main_cloud_service_credential(
                instance_id,
                login,
                ciphertext=crypto.encrypt(minted),
                token_name=app_password_name(instance_id),
                expected_revision=expected,
            )
        except Exception:
            # Never leave a token behind that nobody holds.
            await self._revoke_agent_app_password(minted)
            raise
        if stored:
            self._use_agent_app_password(
                minted, 1 if expected is None else expected + 1
            )
            self._agent_refused_revision = None
            logger.info(
                "Nextcloud agent-service signs in with a new app password "
                "on installation %s",
                instance_id,
            )
            return True
        # Another replica stored one first: use it, and revoke the one
        # minted here.
        row = await store.get_main_cloud_service_credential(instance_id, login)
        secret = _decrypt(row) if row is not None else None
        if row is not None and secret is not None:
            self._use_agent_app_password(secret, int(row["revision"]))
        await self._revoke_agent_app_password(minted)
        if secret is None:
            raise _AgentSecretUnavailable("no stored app password could be read")
        return True

    # ---------------------------------------------------- Nextcloud's side

    def _agent_secret_client(
        self, secret: str, *, user_agent: str
    ) -> httpx.AsyncClient:
        """A fresh agent-service client that stores and sends no cookie."""
        return httpx.AsyncClient(
            base_url=self._base_url,
            auth=(self._agent_user, secret),
            cookies=NoCookieJar(),
            headers={
                "OCS-APIRequest": "true",
                "Accept": "application/json",
                "User-Agent": user_agent,
            },
            timeout=_TIMEOUT,
            follow_redirects=False,
            transport=self._agent_secret_transport,
        )

    async def _mint_agent_app_password(self, instance_id: str) -> str:
        """``GET core/getapppassword`` with the login password; the token is
        named after this request's User-Agent."""
        try:
            async with self._agent_secret_client(
                self._agent_password, user_agent=app_password_name(instance_id)
            ) as client:
                resp = await client.get(_GET_APP_PASSWORD, params={"format": "json"})
        except httpx.HTTPError as exc:
            raise _AgentSecretUnavailable(
                f"the app password mint failed ({type(exc).__name__})"
            ) from None
        if resp.status_code != 200:
            raise _AgentSecretUnavailable(
                f"the app password mint answered HTTP {resp.status_code}"
            )
        try:
            minted = resp.json()["ocs"]["data"]["apppassword"]
        except (ValueError, KeyError, TypeError):
            minted = None
        if not isinstance(minted, str) or not minted:
            raise _AgentSecretUnavailable(
                "the app password mint answered no app password"
            )
        return minted

    async def _revoke_agent_app_password(self, app_password: str) -> None:
        """The documented self-revocation, ``DELETE core/apppassword``
        authenticated with the app password itself. Best effort: a token
        left behind is findable by its name. A 401 means it is gone."""
        try:
            async with self._agent_secret_client(
                app_password, user_agent=AGENT_CLIENT_USER_AGENT
            ) as client:
                resp = await client.delete(_APP_PASSWORD, params={"format": "json"})
        except httpx.HTTPError as exc:
            logger.warning(
                "Nextcloud agent-service app password revoke failed (%s)",
                type(exc).__name__,
            )
            return
        if resp.status_code not in (200, 401):
            logger.warning(
                "Nextcloud agent-service app password revoke answered HTTP %s",
                resp.status_code,
            )


def _decrypt(row: dict[str, Any]) -> Optional[str]:
    try:
        secret = crypto.decrypt(str(row.get("credential_ciphertext") or ""))
    except (crypto.DecryptionError, crypto.EncryptionKeyError):
        return None
    return secret or None


__all__ = [
    "ADOPT_RETRY_SECONDS",
    "AGENT_CLIENT_USER_AGENT",
    "MINT_INTERVAL_SECONDS",
    "AgentServiceAuth",
    "NextcloudAgentServiceCredential",
    "app_password_name",
]
