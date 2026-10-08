"""NextcloudBackend — ``MainCloudBackend`` implementation for Nextcloud.

Phase 1.5 hardening pass over the Phase 1 refactor. Changes vs. Phase 1:

* Soft failures now raise ``CloudBackendError`` with mapped
  ``CloudBackendErrorKind`` values. ``ensure_*`` return types are
  non-Optional — callers either get a ``Handle`` or an exception.
  ``resolve_user_identity`` and ``get_user_home`` still return
  ``Optional[...]`` because "user not found" is a valid state.
* HTML-redirect assertion: if a response arrives with a
  ``text/html`` body we raise ``AUTHENTICATION_FAILED`` instead of
  silently parsing garbage. This guards against the
  ``OCS-APIRequest: true`` header accidentally being dropped.
* User lookup now uses ``/ocs/v2.php/cloud/users/details?search=``
  with a client-side exact-match filter. More reliable than the
  legacy two-pass exact-ID + search strategy.
* ``share_session_folder`` is throttled by a client-side leaky bucket
  at ~15 events / 10 min (below the NC 30.0.10+ server ceiling of
  20/10min).
* ``revoke_session_share`` now issues the real OCS DELETE call.

Uses two Nextcloud APIs:
  * Group Folders API:  ``/index.php/apps/groupfolders/folders``
  * Provisioning API:   ``/ocs/v2.php/cloud/``
"""

from __future__ import annotations

import itertools
import hashlib
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote, unquote, urlencode, urlsplit

import httpx

from orchestrator.services.cloud._propfind import parse_propfind_entries
from orchestrator.services.cloud.base import (
    CanaryFixture,
    CloudMountSubject,
    HealthStatus,
    RcloneMountSpec,
    RoReaderGrant,
    UserHome,
)
from orchestrator.services.cloud.backend_instance_authority import (
    main_cloud_installation_proof_sha256,
)
from orchestrator.services.cloud.capabilities import (
    CloudCapability,
    ProviderCapabilities,
)
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.etag_baseline import (
    PropfindError,
    capture_etag_baseline,
)
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud.handles import (
    GroupId,
    ProjectFolderEntry,
    ProjectFolderHandle,
    SessionFolderHandle,
    ShareHandle,
    UserId,
)
from orchestrator.services.cloud.retry import LeakyBucket
from orchestrator.services.cloud.telemetry import instrument_backend_op
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud.protected_effect_contract import (
    NextcloudEffectFenceIntent,
    adopt_protected_effect_installation_attestation,
)

logger = logging.getLogger(__name__)

BACKEND_ID = "nextcloud"

_ALL_PERMISSIONS = 31  # read(1) + update(2) + create(4) + delete(8) + share(16)
_SESSION_SHARE_PERMISSIONS = 15  # same minus share
_READ_PERMISSION = 1  # read only — the protected-mode RO reader grant

# Nextcloud 30.0.10+ enforces 20 new user shares per 10 minutes per user on
# /ocs/v2.php/apps/files_sharing/api/v1/shares. The client-side bucket sits
# at 15/10min to leave headroom for clock skew and concurrent processes.
_SHARE_RATE_LIMIT_EVENTS = 15
_SHARE_RATE_LIMIT_WINDOW_SECONDS = 600.0

_PROTECTED_EFFECT_CAPABILITY_PATH = (
    "/index.php/apps/srw_protected_effect/api/v1/capability"
)
_PROTECTED_EFFECT_AUTHORITY_HEADER = "X-SRW-Protected-Effect-Authority"
_PROTECTED_EFFECT_SIGNATURE_HEADER = "X-SRW-Protected-Effect-Signature"
_PROTECTED_EFFECT_INSTANCE_HEADER = "X-SRW-Backend-Instance"

_UNRESTRICTABLE_APP_TOKEN = (
    "an app token cannot be restricted to read-only or to one path"
)

#: What Nextcloud delivers (main_cloud_as_connectors.md, "Provider support
#: matrix"). The workspace tier narrows it: rclone mounts the VM tier only
#: with a read-only flag on a write-capable credential, which enforces
#: nothing, so no VM row is offered until the reader grant reaches it.
NEXTCLOUD_CAPABILITIES = ProviderCapabilities(
    backend_id=BACKEND_ID,
    title="Nextcloud",
    capabilities=(
        CloudCapability(
            "cloud_folder",
            "project",
            "read_only",
            "planned",
            "a reader account granted read-only access to this one folder "
            "(the reader grant protected mode mints)",
            frozenset({"sandbox", "vm"}),
            slice=5,
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "read_write",
            "offered",
            "the agent-service account, a member of the project's Group "
            "Folder (it can write every project folder until slice 7)",
            frozenset({"sandbox"}),
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "protected",
            "offered",
            "the reader grant as a read-only lower layer; writes stay in an "
            "overlay until the owner approves them in review",
            frozenset({"sandbox"}),
        ),
        CloudCapability(
            "cloud_folder",
            "user_root",
            "read_write",
            "planned",
            "a per-user app token for the user's own files",
            frozenset({"sandbox"}),
            slice=4,
        ),
        CloudCapability(
            "cloud_folder",
            "user_root",
            "read_only",
            "unsupported",
            _UNRESTRICTABLE_APP_TOKEN,
        ),
        CloudCapability(
            "cloud_folder",
            "user_root",
            "protected",
            "unsupported",
            _UNRESTRICTABLE_APP_TOKEN,
        ),
        CloudCapability(
            "cloud_folder_checkout",
            "project",
            "reviewed_write_back",
            "offered",
            "the folder is copied into the job's repository; changes are "
            "written back only when the reviewer accepts the diff",
            frozenset({"sandbox", "vm"}),
        ),
        CloudCapability(
            "cloud_outbox",
            None,
            "read_write",
            "offered",
            "a folder created for the execution and shared with its owner",
            frozenset({"sandbox"}),
        ),
    ),
)


class NextcloudBackend:
    """Main-cloud backend for Nextcloud.

    Reads configuration from environment variables:
        NEXTCLOUD_URL:            Internal base URL (default: http://localhost:8800)
        NEXTCLOUD_PUBLIC_URL:     Browser-facing URL (default: same as NEXTCLOUD_URL)
        NEXTCLOUD_ADMIN_USER:     Admin username (default: admin)
        NEXTCLOUD_ADMIN_PASSWORD: Admin password (default: admin)
        NEXTCLOUD_AGENT_USER:     Service account username (default: agent-service)
        NEXTCLOUD_AGENT_PASSWORD: Service account password (default: agent-service-dev)
    """

    backend_id = BACKEND_ID
    capabilities = NEXTCLOUD_CAPABILITIES

    def __init__(self, settings: NextcloudSettings) -> None:
        # Phase 1.5 parity with OpenCloudBackend (Issue 12): consume a
        # validated ``NextcloudSettings`` built by ``load_main_cloud_config``
        # in ``build_backend`` rather than reading ``NEXTCLOUD_*`` env vars
        # directly. The settings path honors the ``MAIN_CLOUD_*`` aliases, the
        # DB overlay, and ``credentials_ref`` — all of which the old direct
        # ``os.getenv`` constructor silently ignored.
        self._settings = settings
        self._base_url = str(settings.base_url).rstrip("/")
        self._public_url = str(settings.public_url).rstrip("/")
        self._admin_user = settings.admin_user
        self._admin_password = settings.admin_password.get_secret_value()
        self._agent_user = settings.agent_user
        self._agent_password = settings.agent_password.get_secret_value()
        self._protected_effect_url = (
            str(settings.protected_effect_url).rstrip("/")
            if settings.protected_effect_url is not None
            else None
        )
        self._protected_effect_config_sha256 = settings.protected_effect_config_sha256
        self._protected_effect_hmac_key = (
            settings.protected_effect_hmac_key.get_secret_value().encode("utf-8")
            if settings.protected_effect_hmac_key is not None
            else None
        )
        self._initialized = False
        self._client: Optional[httpx.AsyncClient] = None
        self._protected_effect_client: Optional[httpx.AsyncClient] = None
        self._backend_instance_id: Optional[str] = None
        self._attestation_backend_instance_id: Optional[str] = None
        self._installation_proof_sha256: Optional[str] = None
        self._share_bucket = LeakyBucket(
            max_events=_SHARE_RATE_LIMIT_EVENTS,
            window_seconds=_SHARE_RATE_LIMIT_WINDOW_SECONDS,
        )

    # --------------------------------------------------------------- Properties

    @property
    def is_configured(self) -> bool:
        return bool(self._admin_user and self._admin_password)

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def backend_instance_id(self) -> Optional[str]:
        return self._backend_instance_id

    @property
    def installation_proof_sha256(self) -> Optional[str]:
        return self._installation_proof_sha256

    @property
    def protected_effect_config_sha256(self) -> Optional[str]:
        return self._protected_effect_config_sha256

    @property
    def protected_effect_hmac_key(self) -> Optional[bytes]:
        # Bytes are returned only to the in-process signer. They are never
        # included in plans, persisted authorities, responses, or logs.
        return self._protected_effect_hmac_key

    def bind_backend_instance(self, backend_instance_id: str) -> None:
        """Bind the DB-adopted installation UUID exactly once."""

        if self._backend_instance_id not in {None, backend_instance_id}:
            raise RuntimeError("Nextcloud backend instance authority changed")
        self._backend_instance_id = backend_instance_id
        self._attestation_backend_instance_id = None

    def prepare_backend_instance_attestation(self, backend_instance_id: str) -> None:
        """Supply a proposed UUID without treating it as routing authority."""

        if not isinstance(backend_instance_id, str) or not backend_instance_id:
            raise ValueError("Nextcloud attestation instance is missing")
        if self._backend_instance_id is not None:
            if self._backend_instance_id != backend_instance_id:
                raise RuntimeError("Nextcloud backend instance authority changed")
            return
        if self._attestation_backend_instance_id not in {
            None,
            backend_instance_id,
        }:
            raise RuntimeError("Nextcloud attestation instance changed")
        self._attestation_backend_instance_id = backend_instance_id

    def _protected_effect_origin_path(self, path: str) -> str:
        """Prefix a signed target with the installation path exactly once."""

        if not isinstance(path, str) or not path.startswith("/") or "?" in path:
            raise ValueError("protected Nextcloud effect path is malformed")
        base_path = urlsplit(self._base_url).path.rstrip("/")
        if base_path and (path == base_path or path.startswith(f"{base_path}/")):
            return path
        return f"{base_path}{path}"

    async def fetch_protected_effect_capability(
        self,
    ) -> tuple[dict[str, Any], str]:
        """Fetch the remote app's signed, installation-bound timing contract."""

        self._ensure_ready()
        if self._backend_instance_id is None:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED,
                "protected Nextcloud effect lane has no instance authority",
                backend=self.backend_id,
                retryable=False,
            )
        (
            capability,
            signature,
            _attestation,
            _attestation_signature,
        ) = await self._request_protected_effect_capability(self._backend_instance_id)
        return capability, signature

    async def _request_protected_effect_capability(
        self,
        backend_instance_id: str,
    ) -> tuple[dict[str, Any], str, dict[str, Any], str]:
        """Fetch capability bytes during startup or normal effect dispatch."""

        if (
            self._protected_effect_client is None
            or self._protected_effect_config_sha256 is None
            or self._protected_effect_hmac_key is None
        ):
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED,
                "protected Nextcloud effect lane is not configured",
                backend=self.backend_id,
                retryable=False,
            )
        try:
            response = await self._protected_effect_client.get(
                self._protected_effect_origin_path(_PROTECTED_EFFECT_CAPABILITY_PATH),
                headers={
                    _PROTECTED_EFFECT_INSTANCE_HEADER: backend_instance_id,
                },
            )
            response.raise_for_status()
            self._assert_not_html(response)
            payload = response.json()
        except httpx.HTTPError as exc:
            raise self._map_http_error(exc) from exc
        except (TypeError, ValueError) as exc:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                "protected Nextcloud effect capability response is malformed",
                backend=self.backend_id,
            ) from exc
        if not isinstance(payload, dict) or set(payload) != {
            "capability",
            "signature",
            "installation_attestation",
            "installation_signature",
        }:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                "protected Nextcloud effect capability response is malformed",
                backend=self.backend_id,
            )
        capability = payload.get("capability")
        signature = payload.get("signature")
        attestation = payload.get("installation_attestation")
        attestation_signature = payload.get("installation_signature")
        if (
            not isinstance(capability, dict)
            or not isinstance(signature, str)
            or not isinstance(attestation, dict)
            or not isinstance(attestation_signature, str)
        ):
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                "protected Nextcloud effect capability response is malformed",
                backend=self.backend_id,
            )
        return capability, signature, attestation, attestation_signature

    async def dispatch_protected_effect(
        self,
        intent: NextcloudEffectFenceIntent,
        *,
        body: bytes,
    ) -> httpx.Response:
        """Send one already-durable request to its exact signed target path."""

        self._ensure_ready()
        if (
            not isinstance(intent, NextcloudEffectFenceIntent)
            or self._protected_effect_client is None
            or intent.request.backend_instance_id != self._backend_instance_id
            or intent.request.config_sha256 != self._protected_effect_config_sha256
            or type(body) is not bytes
            or hashlib.sha256(body).hexdigest() != intent.request.body_sha256
        ):
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "protected Nextcloud effect dispatch authority is malformed",
                backend=self.backend_id,
                retryable=False,
            )
        try:
            return await self._protected_effect_client.request(
                intent.request.method,
                intent.request.path,
                content=body,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    _PROTECTED_EFFECT_INSTANCE_HEADER: (
                        intent.request.backend_instance_id
                    ),
                    _PROTECTED_EFFECT_AUTHORITY_HEADER: intent.request.canonical_json,
                    _PROTECTED_EFFECT_SIGNATURE_HEADER: intent.request_signature,
                },
            )
        except httpx.HTTPError as exc:
            raise self._map_http_error(exc) from exc

    @property
    def webdav_credentials(self) -> dict[str, str]:
        """Agent credentials for WebDAV access to Nextcloud folders."""
        return {"username": self._agent_user, "password": self._agent_password}

    def cloud_sync_config(
        self, webdav_url: str, *, target_user_sub: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        """The agent's ``cloud_sync`` entry for one folder: basic auth as the
        agent-service account, or ``None`` without its credentials.

        Nextcloud has no token exchange, so ``target_user_sub`` changes
        nothing. Never logs the auth payload.
        """
        creds = self.webdav_credentials or {}
        if not creds.get("username") or not creds.get("password"):
            return None
        return {
            "backend": self.backend_id,
            "webdav_url": webdav_url,
            "auth": {
                "type": "basic",
                "username": creds["username"],
                "password": creds["password"],
            },
        }

    @staticmethod
    def legacy_folder_id(handle: ProjectFolderHandle) -> Optional[int]:
        """The pre-abstraction ``projects.nextcloud_folder_id``: the Group
        Folder id, while that column is still written."""
        try:
            return int(handle.native_id)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def protected_lower_transport(row: dict[str, Any]) -> dict[str, Any]:
        """The provider half of a protected mount's read-only lower layer.

        Built from an active ``cloud_ro_mounts`` reader grant: rclone WebDAV
        as the per-mount reader account, never the agent-service account.
        The caller adds the overlay layout and the cache settings.
        """
        return {
            "backend": BACKEND_ID,
            "source": {
                "type": "webdav",
                "config": {
                    "url": row["webdav_url"],
                    "vendor": BACKEND_ID,
                    "user": row["reader_id"],
                },
            },
            "auth": {"type": "basic", "password": row["credentials"]},
        }

    def _explicit_user_home_credentials(
        self, handle: ProjectFolderHandle, subject: CloudMountSubject | None
    ) -> dict[str, str] | None:
        """Resolve explicit user-granted credentials for a user-home mount.

        v1 intentionally does not pair ``/remote.php/dav/files/{user}/`` with
        the agent-service password. For homelab/single-user deployments we
        support an explicit app-password-style env pair:

        * ``NEXTCLOUD_RCLONE_USER_HOME_USERNAME``
        * ``NEXTCLOUD_RCLONE_USER_HOME_PASSWORD``

        A username-specific password env is also accepted for multi-user test
        deployments: ``NEXTCLOUD_RCLONE_USER_HOME_PASSWORD_<SANITIZED_USER>``.
        """
        username = (
            handle.vendor_meta.get("username")
            or (subject.username if subject else None)
            or ""
        )
        username = str(username).strip()
        if not username:
            return None

        configured_user = os.getenv("NEXTCLOUD_RCLONE_USER_HOME_USERNAME")
        configured_password = os.getenv("NEXTCLOUD_RCLONE_USER_HOME_PASSWORD")
        if configured_user and configured_password and configured_user == username:
            return {"username": configured_user, "password": configured_password}

        env_suffix = re.sub(r"[^A-Z0-9]+", "_", username.upper()).strip("_")
        if env_suffix:
            user_password = os.getenv(
                f"NEXTCLOUD_RCLONE_USER_HOME_PASSWORD_{env_suffix}"
            )
            if user_password:
                return {"username": username, "password": user_password}
        return None

    async def build_rclone_mount_spec(
        self,
        *,
        handle: ProjectFolderHandle | SessionFolderHandle,
        mount_kind: str,
        target_path: str,
        access: str,
        subject: CloudMountSubject | None = None,
        prefer_public_url: bool = False,
    ) -> RcloneMountSpec:
        """Build an rclone WebDAV spec for a Nextcloud-backed cloud surface."""
        if isinstance(handle, SessionFolderHandle):
            webdav_url = self.get_session_folder_webdav_url(handle)
            creds = self.webdav_credentials
        else:
            if handle.vendor_meta.get("kind") == "user_home":
                username = str(handle.vendor_meta.get("username") or "")
                webdav_url = (
                    f"{self._base_url}/remote.php/dav/files/{quote(username, safe='')}/"
                    if username
                    else None
                )
                creds = self._explicit_user_home_credentials(handle, subject) or {}
            else:
                webdav_url = self.get_project_folder_webdav_url(handle)
                creds = self.webdav_credentials

        # Cross-cluster VM runtimes can't reach the internal service URL; swap
        # to the public edge (no-op unless the URL is internal-prefixed). Mirror
        # of the OpenCloud path — see
        # knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md.
        if (
            prefer_public_url
            and webdav_url
            and self._public_url
            and webdav_url.startswith(self._base_url)
        ):
            webdav_url = self._public_url + webdav_url[len(self._base_url) :]

        if not webdav_url:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                f"cannot build rclone mount for {mount_kind}: missing WebDAV URL",
                backend=self.backend_id,
            )
        if not creds.get("username") or not creds.get("password"):
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED,
                f"cannot build rclone mount for {mount_kind}: missing safe WebDAV credentials",
                backend=self.backend_id,
            )

        return RcloneMountSpec(
            source_type="webdav",
            source_config={
                "url": webdav_url,
                "vendor": "nextcloud",
                "user": creds["username"],
            },
            auth={"type": "basic", "password": creds["password"]},
            cache={
                "vfs_cache_mode": "full",
                "vfs_cache_max_size": "10G",
                "vfs_cache_max_age": "24h",
                "dir_cache_time": "5m",
                "poll_interval": "1m",
                "vfs_read_chunk_size": "16M",
                "vfs_read_chunk_size_limit": "128M",
                "hard_cache_limit": "20G",
            },
        )

    # ---------------------------------------------------------------- Lifecycle

    async def ensure_initialized(self) -> bool:
        """Verify Nextcloud is reachable and ``groupfolders`` is installed.

        Soft-failure semantics are preserved here: this method is called once
        at FastAPI startup and returns ``False`` when Nextcloud is not
        configured or unreachable, so the rest of the orchestrator can boot
        without the backend. After this, every other method raises
        ``CloudBackendError`` on hard failure.
        """
        if not self.is_configured:
            logger.info(
                "Nextcloud backend not configured "
                "(NEXTCLOUD_ADMIN_USER/PASSWORD not set)"
            )
            return False

        try:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                auth=(self._admin_user, self._admin_password),
                headers={
                    "OCS-APIRequest": "true",
                    "User-Agent": "SuperhumanRemoteWorker/0.1 main-cloud-backend",
                },
                timeout=httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=10.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
            if self._protected_effect_url is not None:
                self._protected_effect_client = httpx.AsyncClient(
                    base_url=self._protected_effect_url,
                    auth=(self._admin_user, self._admin_password),
                    headers={
                        "OCS-APIRequest": "true",
                        "Accept": "application/json",
                        "User-Agent": (
                            "SuperhumanRemoteWorker/0.1 protected-effect-lane"
                        ),
                    },
                    timeout=httpx.Timeout(
                        connect=5.0,
                        read=65.0,
                        write=10.0,
                        pool=10.0,
                    ),
                    limits=httpx.Limits(
                        max_connections=4,
                        max_keepalive_connections=2,
                    ),
                )

            resp = await self._client.get("/status.php")
            resp.raise_for_status()
            try:
                status_payload = resp.json()
            except (TypeError, ValueError):
                status_payload = None
            remote_instance_id = (
                status_payload.get("instanceid")
                if isinstance(status_payload, dict)
                else None
            )
            legacy_installation_proof = None
            if isinstance(remote_instance_id, str) and remote_instance_id.strip():
                legacy_installation_proof = main_cloud_installation_proof_sha256(
                    backend_id=self.backend_id,
                    remote_identity=remote_instance_id,
                )

            resp = await self._client.get(
                "/index.php/apps/groupfolders/folders",
                params={"format": "json"},
            )
            if resp.status_code == 404:
                logger.warning(
                    "Nextcloud groupfolders app not installed — "
                    "cloud folder provisioning disabled"
                )
                await self._client.aclose()
                self._client = None
                if self._protected_effect_client is not None:
                    await self._protected_effect_client.aclose()
                    self._protected_effect_client = None
                return False
            resp.raise_for_status()

            self._assert_not_html(resp)  # OCS-APIRequest header sanity check

            # Detect Group Folders bug #4127 — a corrupted root_id crashes
            # the app. If we can parse the response as JSON without error,
            # the app is healthy; otherwise log a clear recovery hint.
            try:
                resp.json()
            except ValueError as e:
                logger.error(
                    "Nextcloud groupfolders endpoint returned non-JSON — "
                    "possible root_id corruption (bug groupfolders#4127). "
                    "Recovery: check `oc_filecache` / `oc_group_folders` "
                    "integrity. Error: %s",
                    e,
                )
                await self._client.aclose()
                self._client = None
                if self._protected_effect_client is not None:
                    await self._protected_effect_client.aclose()
                    self._protected_effect_client = None
                return False

            installation_proof = legacy_installation_proof
            if self._protected_effect_url is not None:
                attestation_instance_id = (
                    self._backend_instance_id or self._attestation_backend_instance_id
                )
                if attestation_instance_id is None:
                    raise RuntimeError(
                        "protected Nextcloud installation attestation has no "
                        "proposed backend instance"
                    )
                client_before = datetime.now(timezone.utc)
                (
                    capability,
                    capability_signature,
                    installation_attestation,
                    installation_signature,
                ) = await self._request_protected_effect_capability(
                    attestation_instance_id
                )
                client_after = datetime.now(timezone.utc)
                signed_proof = adopt_protected_effect_installation_attestation(
                    capability,
                    capability_signature=capability_signature,
                    attestation_binding=installation_attestation,
                    attestation_signature=installation_signature,
                    key=self._protected_effect_hmac_key,
                    client_before=client_before,
                    client_after=client_after,
                    expected_backend_instance_id=attestation_instance_id,
                    expected_config_sha256=self._protected_effect_config_sha256,
                )
                if signed_proof is None:
                    raise RuntimeError(
                        "protected Nextcloud installation attestation is invalid"
                    )
                if (
                    legacy_installation_proof is not None
                    and signed_proof != legacy_installation_proof
                ):
                    raise RuntimeError(
                        "protected Nextcloud installation proofs disagree"
                    )
                installation_proof = signed_proof

            self._installation_proof_sha256 = installation_proof
            if installation_proof is None:
                logger.warning(
                    "Nextcloud exposes no stable installation identity; "
                    "main-cloud authority remains unavailable"
                )

            self._initialized = True
            logger.info("Nextcloud backend initialized (groupfolders available)")
            return True
        except Exception as e:
            logger.warning(f"Nextcloud backend init failed: {e}")
            if self._client:
                await self._client.aclose()
                self._client = None
            if self._protected_effect_client is not None:
                await self._protected_effect_client.aclose()
                self._protected_effect_client = None
            return False

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None
        if self._protected_effect_client is not None:
            try:
                await self._protected_effect_client.aclose()
            finally:
                self._protected_effect_client = None
        self._initialized = False

    async def health_check(self) -> HealthStatus:
        if not self._initialized or self._client is None:
            return HealthStatus(ok=False, latency_ms=0.0, detail="not initialized")
        try:
            start = time.monotonic()
            resp = await self._client.get("/status.php")
            elapsed_ms = (time.monotonic() - start) * 1000
            return HealthStatus(
                ok=resp.status_code == 200,
                latency_ms=elapsed_ms,
                detail=f"status={resp.status_code}",
            )
        except Exception as e:
            return HealthStatus(ok=False, latency_ms=0.0, detail=str(e))

    # ------------------------------------------------------------------ Identity

    @instrument_backend_op("resolve_user_identity")
    async def resolve_user_identity(
        self, email: Optional[str], display_name: Optional[str]
    ) -> Optional[UserId]:
        """Resolve an SSO user to the backend's Nextcloud username.

        Phase 1.5 switches from the legacy two-pass (exact-ID + search) to
        a single ``/users/details?search=`` call with a client-side exact
        match on the email field. Rationale: ``/users/details`` returns
        the full user objects with ``email`` / ``displayname`` fields,
        which lets us disambiguate without trusting a ``len == 1``
        heuristic. Returns ``None`` when no user matches — that is a
        valid state (user hasn't logged in via OIDC yet), not an error.
        """
        self._ensure_ready()
        candidates = [c for c in (email, display_name) if c]
        if not candidates:
            return None
        try:
            for query in candidates:
                resp = await self._client.get(
                    "/ocs/v2.php/cloud/users/details",
                    params={"format": "json", "search": query},
                )
                if resp.status_code != 200:
                    continue
                self._assert_not_html(resp)
                data = resp.json()
                users_obj = data.get("ocs", {}).get("data", {}).get("users", {})
                # /users/details returns a dict keyed by user-id; /users returns a list.
                user_records: list[tuple[str, dict[str, Any]]] = []
                if isinstance(users_obj, dict):
                    user_records = list(users_obj.items())
                elif isinstance(users_obj, list):
                    # Fallback for servers that return the simpler list shape
                    for uid in users_obj:
                        user_records.append((str(uid), {}))
                # Exact-match pass: prefer email match, then display_name match
                if email:
                    for uid, rec in user_records:
                        if rec.get("email", "").lower() == email.lower():
                            return UserId(uid)
                if display_name:
                    for uid, rec in user_records:
                        if rec.get("displayname", "").lower() == display_name.lower():
                            return UserId(uid)
                # Last resort: if the search returned exactly one row, trust it
                if len(user_records) == 1:
                    return UserId(user_records[0][0])
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        return None

    async def ensure_user(
        self,
        *,
        sub: str,
        issuer: str,
        email: Optional[str],
        display_name: Optional[str],
        preferred_username: Optional[str] = None,
    ) -> Optional[UserId]:
        """Nextcloud's user_oidc app provisions on first login; no admin API.

        We don't forge a local account from the service side because
        Nextcloud's OIDC-linked accounts only materialise via the login
        flow. Callers fall back to ``resolve_user_identity`` at share time.
        """
        return await self.resolve_user_identity(email, display_name)

    def get_default_home_browser_url(self) -> Optional[str]:
        """Generic Nextcloud Files app URL (user-agnostic)."""
        return f"{self._public_url}/apps/files/"

    async def get_user_home(self, user_id: UserId) -> Optional[UserHome]:
        """Build URL triple for the user's Nextcloud home directory.

        Pure URL construction — no HTTP call. The ``user_id`` is treated
        as the Nextcloud username.
        """
        if not self._initialized:
            return None
        username = str(user_id)
        webdav_url = f"{self._base_url}/remote.php/dav/files/{username}/"
        browser_url = f"{self._public_url}/apps/files/"
        handle = ProjectFolderHandle(
            backend=self.backend_id,
            native_id=f"home:{username}",
            vendor_meta={"kind": "user_home", "username": username},
        )
        return UserHome(handle=handle, browser_url=browser_url, webdav_url=webdav_url)

    # -------------------------------------------------------------------- Groups

    @instrument_backend_op("ensure_group")
    async def ensure_group(self, group_id: GroupId) -> None:
        """Create a Nextcloud group if it doesn't exist (idempotent)."""
        self._ensure_ready()
        try:
            resp = await self._client.post(
                "/ocs/v2.php/cloud/groups",
                params={"format": "json"},
                data={"groupid": str(group_id)},
            )
            # 200 = created. 400 may mean "already exists" (OCS code 102) —
            # we accept 400 as success after a sanity peek at the envelope.
            if resp.status_code == 200:
                return
            if resp.status_code == 400:
                try:
                    ocs_code = (
                        resp.json().get("ocs", {}).get("meta", {}).get("statuscode")
                    )
                except ValueError:
                    ocs_code = None
                if ocs_code in (None, 102, 104):
                    return  # legacy semantics: treat 400 as already-exists
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("add_user_to_group")
    async def add_user_to_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        try:
            resp = await self._client.post(
                f"/ocs/v2.php/cloud/users/{user_id}/groups",
                params={"format": "json"},
                data={"groupid": str(group_id)},
            )
            resp.raise_for_status()
            logger.debug(f"Added user '{user_id}' to Nextcloud group '{group_id}'")
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("remove_user_from_group")
    async def remove_user_from_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        try:
            # httpx's delete() convenience method rejects a request body, so the
            # groupid must go through request("DELETE", ..., data=...).
            resp = await self._client.request(
                "DELETE",
                f"/ocs/v2.php/cloud/users/{user_id}/groups",
                params={"format": "json"},
                data={"groupid": str(group_id)},
            )
            resp.raise_for_status()
            logger.debug(f"Removed user '{user_id}' from Nextcloud group '{group_id}'")
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    # ------------------------------------------------------------- Project folders

    @instrument_backend_op("ensure_project_folder")
    async def ensure_project_folder(
        self, *, project_name: str, group_id: GroupId
    ) -> ProjectFolderHandle:
        """Create a Group Folder and grant project + srw-agents access."""
        self._ensure_ready()
        try:
            resp = await self._client.post(
                "/index.php/apps/groupfolders/folders",
                params={"format": "json"},
                data={"mountpoint": project_name},
            )
            resp.raise_for_status()
            self._assert_not_html(resp)
            data = resp.json()
            folder_id = data.get("ocs", {}).get("data", {}).get("id")
            if not folder_id:
                raise CloudBackendError(
                    CloudBackendErrorKind.UNKNOWN,
                    "Group folder creation returned no id",
                    backend=self.backend_id,
                    raw=data,
                )

            await self._grant_group_access(
                int(folder_id), str(group_id), _ALL_PERMISSIONS
            )
            await self._grant_group_access(
                int(folder_id), "srw-agents", _ALL_PERMISSIONS
            )

            logger.info(
                f"Created Nextcloud Group Folder '{project_name}' "
                f"(id={folder_id}) for group '{group_id}'"
            )
            return ProjectFolderHandle(
                backend=self.backend_id,
                native_id=str(folder_id),
                vendor_meta={"mountpoint": project_name},
            )
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("delete_project_folder")
    async def delete_project_folder(
        self, handle: ProjectFolderHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        try:
            folder_id = int(handle.native_id)
        except (ValueError, TypeError) as e:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                f"non-integer folder native_id: {handle.native_id!r}",
                backend=self.backend_id,
            ) from e
        try:
            resp = await self._client.delete(
                f"/index.php/apps/groupfolders/folders/{folder_id}",
                params={"format": "json"},
            )
            if resp.status_code == 200:
                logger.info(f"Deleted Nextcloud Group Folder id={folder_id}")
                return
            if resp.status_code == 404:
                if if_exists:
                    logger.debug(f"Group Folder id={folder_id} already gone")
                    return
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_FOUND,
                    f"Group Folder id={folder_id} not found",
                    backend=self.backend_id,
                    status_code=404,
                )
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("refresh_project_folder_access")
    async def refresh_project_folder_access(
        self, handle: ProjectFolderHandle, group_id: GroupId
    ) -> None:
        """Workaround for Nextcloud server#57445.

        Removes and re-adds the group on the folder to force share
        propagation to new group members. No-op on non-Nextcloud backends.
        """
        self._ensure_ready()
        try:
            folder_id = int(handle.native_id)
        except (ValueError, TypeError) as e:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                f"non-integer folder native_id: {handle.native_id!r}",
                backend=self.backend_id,
            ) from e
        try:
            await self._client.delete(
                f"/index.php/apps/groupfolders/folders/{folder_id}/groups/{group_id}",
                params={"format": "json"},
            )
            await self._grant_group_access(folder_id, str(group_id), _ALL_PERMISSIONS)
            logger.debug(
                f"Refreshed group folder access: folder={folder_id}, group={group_id}"
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    def get_project_folder_browser_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        """Browser deep-link to a Group Folder in Nextcloud."""
        mountpoint = handle.vendor_meta.get("mountpoint")
        if not mountpoint:
            return None
        return f"{self._public_url}/apps/files/?dir=/{quote(mountpoint)}"

    def get_project_folder_webdav_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        """Internal WebDAV URL for agent access to a Group Folder."""
        mountpoint = handle.vendor_meta.get("mountpoint")
        if not mountpoint:
            return None
        return (
            f"{self._base_url}/remote.php/dav/groupfolders/"
            f"{self._agent_user}/{mountpoint}/"
        )

    def _groupfolder_dav_base(self, handle: ProjectFolderHandle) -> str:
        """WebDAV base path (no trailing slash) for a Group Folder's bytes.

        ``agent-service`` is a member of ``srw-agents``, which has access to
        every project Group Folder, so it reaches the folder's contents via
        the groupfolders DAV endpoint keyed by mountpoint — the same path
        ``get_project_folder_webdav_url`` exposes to agents. The mountpoint
        lives in the handle's ``vendor_meta`` (stamped at create time by
        ``ensure_project_folder``).
        """
        mountpoint = handle.vendor_meta.get("mountpoint")
        if not mountpoint:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "project folder handle has no mountpoint "
                f"(native_id={handle.native_id!r})",
                backend=self.backend_id,
                retryable=False,
            )
        return (
            f"/remote.php/dav/groupfolders/{self._agent_user}/"
            f"{quote(mountpoint, safe='')}"
        )

    async def list_project_folder(
        self,
        handle: ProjectFolderHandle,
        *,
        subpath: str = "",
    ) -> list[ProjectFolderEntry]:
        """Recursive enumeration of a Group Folder via repeated ``Depth: 1``
        PROPFIND.

        sabre/dav disables ``Depth: infinity`` by default, so — like the
        OpenCloud backend — we walk the tree breadth-first, one ``Depth: 1``
        PROPFIND per directory. Returns every descendant (files +
        directories); order is breadth-first.
        """
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        # Strip this prefix off every href so callers get paths relative to
        # the folder root.
        href_prefix = f"{base_path}/"

        all_entries: list[ProjectFolderEntry] = []
        # Breadth-first queue of subpaths to expand. Empty string = root.
        queue: list[str] = [subpath.strip("/")]
        seen: set[str] = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            safe_sub = quote(current, safe="/")
            url_path = f"{base_path}/{safe_sub}" if safe_sub else base_path
            # Pass self_path=current so the subdir's own self-entry is dropped
            # by the parser (it was already emitted as a child of its parent);
            # without it, every subdirectory is counted twice (design §11.5).
            entries = await self._propfind_depth_one(
                url_path, href_prefix, self_path=current
            )
            for entry in entries:
                all_entries.append(entry)
                if entry.is_dir and entry.path not in seen:
                    queue.append(entry.path)
        return all_entries

    async def _propfind_depth_one(
        self, url_path: str, href_prefix: str, *, self_path: str = ""
    ) -> list[ProjectFolderEntry]:
        """Single ``Depth: 1`` PROPFIND that returns one folder's children.

        Sent with no body — sabre/dav returns the standard live properties
        (``resourcetype``, ``getcontentlength``, ``getetag``,
        ``getcontenttype``) by default, which is what
        ``parse_propfind_entries`` reads.
        """
        try:
            resp = await self._client.request(
                "PROPFIND",
                url_path,
                headers={"Depth": "1"},
                auth=(self._agent_user, self._agent_password),
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        if resp.status_code == 404:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"path {url_path} not found",
                backend=self.backend_id,
                status_code=404,
            )
        if resp.status_code != 207:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                f"unexpected PROPFIND status {resp.status_code} for {url_path}",
                backend=self.backend_id,
                status_code=resp.status_code,
                raw={"body": resp.text[:500]},
            )
        return parse_propfind_entries(
            resp.text, href_prefix=href_prefix, self_path=self_path
        )

    async def _propfind_at_depth(
        self, url_path: str, href_prefix: str, *, depth: str, self_path: str = ""
    ) -> list[ProjectFolderEntry]:
        """PROPFIND at an explicit Depth. ``depth="infinity"`` powers the
        one-shot etag baseline; a non-207 (e.g. sabre's 400/403 on infinity)
        or a transport error raises ``PropfindError`` so the caller falls back
        to a Depth:1 BFS (design §11.5)."""
        try:
            resp = await self._client.request(
                "PROPFIND",
                url_path,
                headers={"Depth": depth},
                auth=(self._agent_user, self._agent_password),
            )
        except httpx.HTTPError as e:
            raise PropfindError(str(e)) from e
        if resp.status_code != 207:
            raise PropfindError(
                f"PROPFIND Depth:{depth} returned {resp.status_code} for {url_path}"
            )
        return parse_propfind_entries(
            resp.text, href_prefix=href_prefix, self_path=self_path
        )

    async def capture_etag_baseline(
        self, handle: ProjectFolderHandle
    ) -> dict[str, str]:
        """Mount-time ``{path: etag}`` baseline for the conflict gate — one
        ``Depth: infinity`` PROPFIND when the server allows it, else a bounded
        Depth:1 BFS (design §3.4, §11.5)."""
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        href_prefix = f"{base_path}/"

        async def list_tree() -> list[ProjectFolderEntry]:
            return await self._propfind_at_depth(
                base_path, href_prefix, depth="infinity"
            )

        async def list_children(sub: str) -> list[ProjectFolderEntry]:
            safe_sub = quote(sub, safe="/")
            url_path = f"{base_path}/{safe_sub}" if safe_sub else base_path
            return await self._propfind_at_depth(
                url_path, href_prefix, depth="1", self_path=sub
            )

        return await capture_etag_baseline(
            root_subpath="", list_children=list_children, list_tree=list_tree
        )

    @instrument_backend_op("get_project_folder_file_bytes")
    async def get_project_folder_file_bytes(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
    ) -> bytes:
        """GET one file out of a Group Folder."""
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        rel = path.lstrip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "get_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        url = f"{base_path}/{quote(rel, safe='/')}"
        try:
            resp = await self._client.get(
                url, auth=(self._agent_user, self._agent_password)
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        if resp.status_code == 404:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"file {path!r} not found in group folder {handle.native_id}",
                backend=self.backend_id,
                status_code=404,
            )
        if resp.status_code != 200:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                f"GET file {path!r}: unexpected status {resp.status_code}",
                backend=self.backend_id,
                status_code=resp.status_code,
            )
        return resp.content

    @instrument_backend_op("put_project_folder_file_bytes")
    async def put_project_folder_file_bytes(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> None:
        """PUT one file into a Group Folder, MKCOL-ing parents on the way.

        Same MKCOL-each-segment-then-PUT idiom as ``put_session_file``.
        Used by the Mode A accept flow to write the agent's accepted edits
        back to the cloud folder.
        """
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        segments = rel.split("/")
        parents = segments[:-1]
        try:
            for i in range(len(parents)):
                partial = "/".join(parents[: i + 1])
                url = f"{base_path}/{quote(partial, safe='/')}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    auth=(self._agent_user, self._agent_password),
                )
                # 201 = created, 405 = already exists — both fine.
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()

            put_url = f"{base_path}/{quote(rel, safe='/')}"
            headers: dict[str, str] = {}
            if content_type:
                headers["Content-Type"] = content_type
            put_resp = await self._client.put(
                put_url,
                headers=headers,
                content=content,
                auth=(self._agent_user, self._agent_password),
            )
            if put_resp.status_code not in (200, 201, 204):
                put_resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("delete_project_folder_file")
    async def delete_project_folder_file(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
        if_exists: bool = True,
    ) -> None:
        """DELETE one file from a Group Folder."""
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "delete_project_folder_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        url = f"{base_path}/{quote(rel, safe='/')}"
        try:
            resp = await self._client.delete(
                url, auth=(self._agent_user, self._agent_password)
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        if resp.status_code in (200, 204):
            return
        if resp.status_code == 404:
            if if_exists:
                return
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"file {path!r} not found in group folder {handle.native_id}",
                backend=self.backend_id,
                status_code=404,
            )
        raise CloudBackendError(
            CloudBackendErrorKind.UNKNOWN,
            f"DELETE file {path!r}: unexpected status {resp.status_code}",
            backend=self.backend_id,
            status_code=resp.status_code,
        )

    # ------------------------------------------------------------- Session folders

    @instrument_backend_op("ensure_session_folder")
    async def ensure_session_folder(self, *, session_id: str) -> SessionFolderHandle:
        """Create a session folder under the agent-service user's home."""
        self._ensure_ready()
        folder_path = f"sessions/{session_id}"
        try:
            parts = folder_path.strip("/").split("/")
            for i in range(len(parts)):
                partial = "/".join(parts[: i + 1])
                url = f"/remote.php/dav/files/{self._agent_user}/{partial}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    auth=(self._agent_user, self._agent_password),
                )
                # 201 = created, 405 = already exists — both fine.
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()
            logger.info(f"Created session folder: {folder_path}")
            return SessionFolderHandle(
                backend=self.backend_id,
                native_id=folder_path,
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("delete_session_folder")
    async def delete_session_folder(
        self, handle: SessionFolderHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        folder_path = handle.native_id
        try:
            url = f"/remote.php/dav/files/{self._agent_user}/{folder_path}"
            resp = await self._client.delete(
                url, auth=(self._agent_user, self._agent_password)
            )
            if resp.status_code == 204:
                logger.info(f"Deleted session folder: {folder_path}")
                return
            if resp.status_code == 404:
                if if_exists:
                    logger.debug(f"Session folder '{folder_path}' already gone")
                    return
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_FOUND,
                    f"Session folder '{folder_path}' not found",
                    backend=self.backend_id,
                    status_code=404,
                )
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("share_session_folder")
    async def share_session_folder(
        self, handle: SessionFolderHandle, user_id: UserId
    ) -> ShareHandle:
        """Share a session folder with a user via OCS Share API.

        Rate-limited via a client-side leaky bucket to stay under the
        NC 30.0.10+ ``20 shares / 10 min`` server ceiling.
        """
        self._ensure_ready()
        folder_path = handle.native_id
        await self._share_bucket.acquire()
        try:
            resp = await self._client.post(
                "/ocs/v2.php/apps/files_sharing/api/v1/shares",
                params={"format": "json"},
                data={
                    "path": f"/{folder_path}",
                    "shareType": "0",  # 0 = user share
                    "shareWith": str(user_id),
                    "permissions": str(_SESSION_SHARE_PERMISSIONS),
                },
                auth=(self._agent_user, self._agent_password),
            )
            resp.raise_for_status()
            self._assert_not_html(resp)
            data = resp.json()
            share_id = data.get("ocs", {}).get("data", {}).get("id")
            if not share_id:
                raise CloudBackendError(
                    CloudBackendErrorKind.UNKNOWN,
                    "OCS share response missing id",
                    backend=self.backend_id,
                    raw=data,
                )
            logger.info(
                f"Shared session folder '{folder_path}' with user '{user_id}' "
                f"(share_id={share_id})"
            )
            return ShareHandle(
                backend=self.backend_id,
                native_id=str(share_id),
            )
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("revoke_session_share")
    async def revoke_session_share(
        self, share: ShareHandle, *, if_exists: bool = True
    ) -> None:
        """Revoke a session-folder share via OCS DELETE."""
        self._ensure_ready()
        try:
            share_id = int(share.native_id)
        except (ValueError, TypeError) as e:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                f"non-integer share native_id: {share.native_id!r}",
                backend=self.backend_id,
            ) from e
        try:
            resp = await self._client.delete(
                f"/ocs/v2.php/apps/files_sharing/api/v1/shares/{share_id}",
                params={"format": "json"},
                auth=(self._agent_user, self._agent_password),
            )
            if resp.status_code == 200:
                logger.info(f"Revoked session share id={share_id}")
                return
            if resp.status_code == 404:
                if if_exists:
                    logger.debug(f"Session share id={share_id} already gone")
                    return
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_FOUND,
                    f"Share id={share_id} not found",
                    backend=self.backend_id,
                    status_code=404,
                )
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    def get_session_folder_browser_url(
        self, handle: SessionFolderHandle
    ) -> Optional[str]:
        """Browser URL for a user to view a shared session folder."""
        folder_name = handle.native_id.rstrip("/").split("/")[-1]
        return f"{self._public_url}/apps/files/?dir=/{quote(folder_name)}"

    def get_session_folder_webdav_url(
        self, handle: SessionFolderHandle
    ) -> Optional[str]:
        """Internal WebDAV URL for agent access to a session folder."""
        return (
            f"{self._base_url}/remote.php/dav/files/"
            f"{self._agent_user}/{handle.native_id}/"
        )

    @instrument_backend_op("put_session_file")
    async def put_session_file(
        self,
        handle: SessionFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> None:
        """PUT one file into a session folder, MKCOL-ing parents on the way."""
        self._ensure_ready()
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_session_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        folder_path = handle.native_id.strip("/")
        full_segments = folder_path.split("/") + rel.split("/")
        parents = full_segments[:-1]
        try:
            for i in range(len(parents)):
                partial = "/".join(parents[: i + 1])
                url = (
                    f"/remote.php/dav/files/{self._agent_user}/"
                    f"{quote(partial, safe='/')}"
                )
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    auth=(self._agent_user, self._agent_password),
                )
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()

            put_url = (
                f"/remote.php/dav/files/{self._agent_user}/"
                f"{quote('/'.join(full_segments), safe='/')}"
            )
            headers: dict[str, str] = {}
            if content_type:
                headers["Content-Type"] = content_type
            put_resp = await self._client.put(
                put_url,
                headers=headers,
                content=content,
                auth=(self._agent_user, self._agent_password),
            )
            if put_resp.status_code not in (200, 201, 204):
                put_resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    # ------------------------------------------------------------ Internal helpers

    def _ensure_ready(self) -> None:
        """Raise UNAVAILABLE if the backend is not initialized."""
        if not self._initialized or self._client is None:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "Nextcloud backend not initialized",
                backend=self.backend_id,
                retryable=False,
            )

    def _assert_not_html(self, resp: httpx.Response) -> None:
        """Catch OCS-APIRequest header drops.

        When the OCS-APIRequest header is missing Nextcloud redirects to
        ``/login`` which httpx follows, yielding a 200 with an HTML body
        that looks like success. This guard converts that class of
        silent failure into an explicit AUTHENTICATION_FAILED error.
        """
        ct = resp.headers.get("content-type", "")
        if "text/html" in ct.lower():
            raise CloudBackendError(
                CloudBackendErrorKind.AUTHENTICATION_FAILED,
                "Nextcloud returned HTML — likely an auth redirect "
                "(missing OCS-APIRequest header or expired session)",
                backend=self.backend_id,
                status_code=resp.status_code,
                raw={"content_type": ct, "text": resp.text[:500]},
            )

    def _map_http_error(self, exc: Exception) -> CloudBackendError:
        """Map an ``httpx`` exception to a ``CloudBackendError``."""
        if isinstance(exc, httpx.HTTPStatusError):
            resp = exc.response
            status = resp.status_code
            kind_map = {
                400: CloudBackendErrorKind.INVALID_REQUEST,
                401: CloudBackendErrorKind.AUTHENTICATION_FAILED,
                403: CloudBackendErrorKind.PERMISSION_DENIED,
                404: CloudBackendErrorKind.NOT_FOUND,
                409: CloudBackendErrorKind.ALREADY_EXISTS,
                413: CloudBackendErrorKind.QUOTA_EXCEEDED,
                429: CloudBackendErrorKind.THROTTLED,
            }
            kind = kind_map.get(status)
            if kind is None:
                kind = (
                    CloudBackendErrorKind.UNAVAILABLE
                    if 500 <= status < 600
                    else CloudBackendErrorKind.UNKNOWN
                )
            retryable = kind in (
                CloudBackendErrorKind.THROTTLED,
                CloudBackendErrorKind.UNAVAILABLE,
                CloudBackendErrorKind.TIMEOUT,
            )
            raw: Optional[dict[str, Any]]
            try:
                raw = resp.json()
            except ValueError:
                raw = {"text": resp.text[:500]}
            request_id = resp.headers.get("x-request-id") or resp.headers.get(
                "x-nextcloud-request-id"
            )
            return CloudBackendError(
                kind,
                f"HTTP {status} from {resp.request.method} {resp.request.url}",
                backend=self.backend_id,
                status_code=status,
                request_id=request_id,
                raw=raw,
                retryable=retryable,
            )
        if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout)):
            return CloudBackendError(
                CloudBackendErrorKind.TIMEOUT,
                f"{type(exc).__name__}: {exc}",
                backend=self.backend_id,
                retryable=True,
            )
        if isinstance(exc, httpx.ConnectError):
            return CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                f"connection error: {exc}",
                backend=self.backend_id,
                retryable=True,
            )
        if isinstance(exc, httpx.RequestError):
            return CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                f"request error: {type(exc).__name__}: {exc}",
                backend=self.backend_id,
                retryable=True,
            )
        return CloudBackendError(
            CloudBackendErrorKind.UNKNOWN,
            f"{type(exc).__name__}: {exc}",
            backend=self.backend_id,
        )

    async def _grant_group_access(
        self, folder_id: int, group_id: str, permissions: int
    ) -> None:
        assert self._client is not None  # guarded by _ensure_ready
        resp = await self._client.post(
            f"/index.php/apps/groupfolders/folders/{folder_id}/groups",
            params={"format": "json"},
            data={"group": group_id},
        )
        resp.raise_for_status()

        resp = await self._client.post(
            f"/index.php/apps/groupfolders/folders/{folder_id}/groups/{group_id}",
            params={"format": "json"},
            data={"permissions": str(permissions)},
        )
        resp.raise_for_status()

    # ------------------------------------------------ protected cloud mode: RO reader

    @staticmethod
    def _ocs_statuscode(resp: httpx.Response) -> Optional[int]:
        try:
            payload = resp.json()
            if not isinstance(payload, dict):
                return None
            ocs = payload.get("ocs")
            if not isinstance(ocs, dict):
                return None
            meta = ocs.get("meta")
            if not isinstance(meta, dict):
                return None
            code = meta.get("statuscode")
            return code if type(code) is int else None
        except (AttributeError, TypeError, ValueError):
            return None

    def _require_ocs_status(
        self, resp: httpx.Response, *, ok: set[int], op: str
    ) -> None:
        """Assert an OCS response is one of ``ok`` (by envelope statuscode), else
        raise. HTTP success without an exact OCS status is not effect proof."""
        code = self._ocs_statuscode(resp)
        if code in ok:
            return
        raise CloudBackendError(
            CloudBackendErrorKind.UNKNOWN,
            f"{op} failed: HTTP {resp.status_code}, ocs statuscode {code}",
            backend=self.backend_id,
            status_code=resp.status_code,
        )

    async def _delete_group(self, group_id: str) -> None:
        resp = await self._client.delete(
            f"/ocs/v2.php/cloud/groups/{group_id}", params={"format": "json"}
        )
        # Nextcloud 31 returns HTTP 400 + OCS 101 for an absent group. Older
        # releases used 998/404. Both are exact "already gone" evidence on
        # this DELETE-only path, so revoke remains idempotent across versions.
        if resp.status_code == 404:
            return
        self._require_ocs_status(
            resp,
            ok={100, 101, 200, 998},
            op=f"delete RO grant group {group_id}",
        )

    async def _dispatch_protected_effect(
        self,
        dispatch: Callable[[str, str, bytes], Awaitable[httpx.Response]],
        *,
        method: str,
        path: str,
        form: dict[str, str],
        ok: set[int],
        op: str,
    ) -> None:
        """Dispatch one authority-creating request through the signed lane.

        The callback is responsible for persisting the exact pre-effect intent,
        sending the canonical path with no query, and durably closing its causal
        horizon.  Keeping the raw ``httpx`` client out of this helper makes it
        impossible for protected-reader creation to silently fall back to an
        ordinary admin request.
        """

        body = urlencode(form).encode("ascii")
        try:
            response = await dispatch(method, path, body)
        except httpx.HTTPError as exc:
            raise self._map_http_error(exc) from exc
        if not isinstance(response, httpx.Response):
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                f"{op} returned no authenticated response",
                backend=self.backend_id,
            )
        self._require_ocs_status(response, ok=ok, op=op)

    async def grant_protected_reader_attempt(
        self,
        plan: ProtectedNextcloudReaderGrantPlan,
        *,
        credentials: str,
        dispatch_effect: Callable[[str, str, bytes], Awaitable[httpx.Response]],
    ) -> RoReaderGrant:
        """Create one attempt-unique reader and its read-only folder grant.

        ``credentials`` is generated and encrypted in the durable engage row
        before this method is called.  The same value is used on every retry;
        there is no post-grant password rotation and therefore no window in
        which a delayed attempt can invalidate another thread's credential.
        Every POST is forced through ``dispatch_effect`` so no authority can be
        created without append-once request evidence and an attested deadline.
        """

        self._ensure_ready()
        grant = self.build_protected_reader_grant(
            plan,
            credentials=credentials,
        )
        if not callable(dispatch_effect):
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "protected reader attempt authority is incomplete",
                backend=self.backend_id,
                retryable=False,
            )

        await self._dispatch_protected_effect(
            dispatch_effect,
            method="POST",
            path=self._protected_effect_origin_path("/ocs/v2.php/cloud/users"),
            form={"userid": plan.reader_id, "password": credentials},
            ok={100, 200, 102},
            op="create protected reader attempt",
        )
        await self._dispatch_protected_effect(
            dispatch_effect,
            method="POST",
            path=self._protected_effect_origin_path("/ocs/v2.php/cloud/groups"),
            form={"groupid": plan.group_id},
            ok={100, 200, 102},
            op="create protected reader grant group",
        )
        await self._dispatch_protected_effect(
            dispatch_effect,
            method="POST",
            path=self._protected_effect_origin_path(
                f"/index.php/apps/groupfolders/folders/{plan.folder_id}/groups"
            ),
            form={"group": plan.group_id},
            ok={100, 200},
            op="attach protected grant group to folder",
        )
        # Groupfolders initially creates a broad ACL.  Membership is added only
        # after this exact downgrade has succeeded, so the reader never observes
        # write authority even if the process dies between the two requests.
        await self._dispatch_protected_effect(
            dispatch_effect,
            method="POST",
            path=self._protected_effect_origin_path(
                f"/index.php/apps/groupfolders/folders/{plan.folder_id}/groups/"
                f"{plan.group_id}"
            ),
            form={"permissions": str(_READ_PERMISSION)},
            ok={100, 200},
            op="set protected reader grant read-only",
        )
        await self._dispatch_protected_effect(
            dispatch_effect,
            method="POST",
            path=self._protected_effect_origin_path(
                f"/ocs/v2.php/cloud/users/{plan.reader_id}/groups"
            ),
            form={"groupid": plan.group_id},
            ok={100, 200, 102},
            op="attach protected reader to grant group",
        )

        return grant

    def build_protected_reader_grant(
        self,
        plan: ProtectedNextcloudReaderGrantPlan,
        *,
        credentials: str,
    ) -> RoReaderGrant:
        """Build the exact credential projection without causing an effect."""

        if (
            not isinstance(plan, ProtectedNextcloudReaderGrantPlan)
            or plan.backend_instance_id != self._backend_instance_id
            or not isinstance(credentials, str)
            or not credentials
        ):
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "protected reader attempt authority is incomplete",
                backend=self.backend_id,
                retryable=False,
            )
        return RoReaderGrant(
            reader_id=plan.reader_id,
            grant_handle=plan.grant_handle,
            webdav_url=(
                f"{self._base_url}/remote.php/dav/files/{plan.reader_id}/"
                f"{quote(plan.mountpoint, safe='')}/"
            ),
            credentials=credentials,
            auth_kind="basic",
        )

    async def _delete_user(self, reader_id: str) -> None:
        response = await self._client.delete(
            f"/ocs/v2.php/cloud/users/{reader_id}",
            params={"format": "json"},
        )
        if response.status_code == 404:
            return
        self._require_ocs_status(
            response,
            # Nextcloud 31: HTTP 400 + OCS 101 means this exact user is
            # absent. See the matching group cleanup above.
            ok={100, 101, 200, 998},
            op=f"delete protected reader {reader_id}",
        )

    async def revoke_protected_reader_attempt(
        self,
        plan: ProtectedNextcloudReaderGrantPlan,
    ) -> None:
        """Idempotently remove exactly one attempt's group and principal.

        Access is removed before the credential principal.  Both identities are
        full-attempt-derived and the canonical plan has already been checked
        against durable backend/source authority, so a delayed A cleanup cannot
        address B even when both attempts belong to the same user or thread.
        """

        self._ensure_ready()
        if (
            not isinstance(plan, ProtectedNextcloudReaderGrantPlan)
            or plan.backend_instance_id != self._backend_instance_id
        ):
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "protected reader revoke authority is incomplete",
                backend=self.backend_id,
                retryable=False,
            )
        await self._delete_group(plan.group_id)
        await self._delete_user(plan.reader_id)

    async def seed_canary_fixture(self, handle: ProjectFolderHandle) -> CanaryFixture:
        """Write a real canary file with the WRITE identity and discover REAL
        version/trash ids so the RO probe's CVE side channels target ids the
        server actually knows (design §11.4). A ref left None keeps that side
        channel inconclusive → the strict engage gate refuses (fail-closed)."""
        path = ".srw-ro-canary/probe.txt"
        fileid = await self._put_canary_and_get_fileid(handle, path)
        version_ref = await self._enumerate_version_ref(fileid) if fileid else None
        trash_ref = await self._seed_and_enumerate_trash_ref(handle)
        return CanaryFixture(path=path, version_ref=version_ref, trash_ref=trash_ref)

    async def remove_canary_fixture(
        self, handle: ProjectFolderHandle, fixture: CanaryFixture
    ) -> None:
        try:
            await self.delete_project_folder_file(
                handle, path=fixture.path, if_exists=True
            )
        except CloudBackendError:
            pass
        # Permanently purge the trash-restore canary so protected-mode engages
        # don't accumulate trashed items. Best-effort — a lingering item only
        # wastes trash space, never affects correctness.
        if fixture.trash_ref:
            user = self.webdav_credentials.get("username")
            if user:
                purge_url = (
                    f"/remote.php/dav/trashbin/{user}/trash/"
                    f"{quote(fixture.trash_ref, safe='')}"
                )
                try:
                    await self._client.request(
                        "DELETE",
                        purge_url,
                        auth=(self._agent_user, self._agent_password),
                    )
                except httpx.HTTPError:
                    pass

    async def _put_canary_and_get_fileid(
        self, handle: ProjectFolderHandle, path: str
    ) -> Optional[str]:
        """PUT the canary (write identity) and return its numeric OC-FileId."""
        resp = await self._put_project_folder_file_bytes_raw(
            handle, path=path, content=b"canary"
        )
        fid = resp.headers.get("OC-FileId") if resp is not None else None
        if not fid:
            return None
        # Real OC-FileId headers are `{fileid}{instance-suffix}` (e.g.
        # "137occ7ab92kf") where the suffix can itself contain digits — a
        # naive "collect every digit char" (the original bug here) would
        # read that as "137792" instead of "137". The versions API keys on
        # the LEADING numeric run only, so take the leading digit run and
        # stop at the first non-digit.
        digits = "".join(itertools.takewhile(str.isdigit, fid))
        return digits or None

    async def _put_project_folder_file_bytes_raw(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> Optional[httpx.Response]:
        """Same MKCOL-parents-then-PUT idiom as ``put_project_folder_file_bytes``,
        but returns the PUT response so the caller can read response headers
        (``OC-FileId``) — the public method discards it and returns ``None``.
        """
        self._ensure_ready()
        base_path = self._groupfolder_dav_base(handle)
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "_put_project_folder_file_bytes_raw: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        segments = rel.split("/")
        parents = segments[:-1]
        try:
            for i in range(len(parents)):
                partial = "/".join(parents[: i + 1])
                url = f"{base_path}/{quote(partial, safe='/')}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    auth=(self._agent_user, self._agent_password),
                )
                # 201 = created, 405 = already exists — both fine.
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()

            put_url = f"{base_path}/{quote(rel, safe='/')}"
            headers: dict[str, str] = {}
            if content_type:
                headers["Content-Type"] = content_type
            put_resp = await self._client.put(
                put_url,
                headers=headers,
                content=content,
                auth=(self._agent_user, self._agent_password),
            )
            if put_resp.status_code not in (200, 201, 204):
                put_resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        return put_resp

    async def _enumerate_version_ref(self, fileid: str) -> Optional[str]:
        """Return ``{fileid}/{versionid}`` for the newest version, or None.

        A brand-new single-write file may have zero versions; that's fine —
        None keeps the versions side channel inconclusive (fail-closed)."""
        user = self.webdav_credentials.get("username")
        if not user:
            return None
        url = f"/remote.php/dav/versions/{user}/versions/{fileid}"
        try:
            resp = await self._client.request(
                "PROPFIND",
                url,
                headers={"Depth": "1"},
                auth=(self._agent_user, self._agent_password),
            )
        except httpx.HTTPError:
            return None
        if resp.status_code != 207:
            return None
        version_id = self._first_child_leaf(resp.text, self_segment=fileid)
        return f"{fileid}/{version_id}" if version_id else None

    # Basename of the throwaway file the trash canary seeds + deletes. The
    # trash-item name it produces is ``{this}.d{unix_ts}`` and is identical in
    # every groupfolder member's trash view, so the RO reader can target it.
    _TRASH_CANARY_NAME = "srw-ro-trash-canary.txt"

    async def _seed_and_enumerate_trash_ref(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        """Seed a real groupfolder-trashed item and return its trash-item name.

        The RO trash-restore CVE (GHSA-2vrq-fhmf-c49m) is a groupfolder member
        restoring a *team-folder*-trashed file — NOT a personal-files item. So
        create the precondition deterministically: PUT a throwaway file into
        the folder (write identity), DELETE it so it lands in the shared trash,
        then read the write identity's trashbin for the item. Its name
        (``{basename}.d{ts}``) is what every member — including the RO reader —
        sees for that deletion, so the reader's restore attempt can target it
        (live-confirmed on NC 31, 2026-07-12). Returns None if any step fails,
        leaving the side channel inconclusive → the strict gate refuses
        (fail-closed).
        """
        user = self.webdav_credentials.get("username")
        if not user:
            return None
        try:
            await self._put_project_folder_file_bytes_raw(
                handle, path=self._TRASH_CANARY_NAME, content=b"trash-canary"
            )
            await self.delete_project_folder_file(
                handle, path=self._TRASH_CANARY_NAME, if_exists=True
            )
        except CloudBackendError:
            return None
        url = f"/remote.php/dav/trashbin/{user}/trash"
        try:
            resp = await self._client.request(
                "PROPFIND",
                url,
                headers={"Depth": "1"},
                auth=(self._agent_user, self._agent_password),
            )
        except httpx.HTTPError:
            return None
        if resp.status_code != 207:
            return None
        hrefs = re.findall(r"<d:href>([^<]+)</d:href>", resp.text, flags=re.IGNORECASE)
        leaves = [unquote(h.rstrip("/").split("/")[-1]) for h in hrefs]
        # `{basename}.d{ts}` — newest (max) is the item we just seeded.
        matches = sorted(
            leaf for leaf in leaves if leaf.startswith(f"{self._TRASH_CANARY_NAME}.d")
        )
        return matches[-1] if matches else None

    @staticmethod
    def _first_child_leaf(xml: str, *, self_segment: Optional[str]) -> Optional[str]:
        """Return the trailing path segment of the first <d:href> that is a
        strict child (not the collection self-href). Used to pull a real
        version id or trashed-item name out of a Depth:1 PROPFIND body.

        A Depth:1 PROPFIND always re-states the queried collection itself as
        its own first ``<d:response>`` (WebDAV semantics; see the same
        self-entry filtering in ``_propfind.parse_propfind_entries``) — so
        without this filter the "first href" is the collection's own
        self-href, not a real child, on every live server response.
        """
        hrefs = re.findall(r"<d:href>([^<]+)</d:href>", xml, flags=re.IGNORECASE)
        for href in hrefs:
            seg = href.rstrip("/").rsplit("/", 1)[-1]
            if not seg:
                continue
            if self_segment is not None and seg == self_segment:
                continue  # the collection self-href
            return seg
        return None
