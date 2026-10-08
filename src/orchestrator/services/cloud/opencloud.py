"""OpenCloudBackend — ``MainCloudBackend`` implementation for OpenCloud.

Speaks LibreGraph (the Microsoft-Graph subset under ``/graph/v1.0/`` and
``/graph/v1beta1/``) exclusively. The legacy OCS endpoints that both
OpenCloud and Nextcloud expose are deliberately NOT used — the Graph
surface is the strategic direction upstream and unifies cleanly with the
future MS365 adapter.

Authenticates as a service account via Keycloak's ``client_credentials``
OAuth2 flow (NOT OpenCloud's internal ``OCIS_SERVICE_ACCOUNT_*`` mechanism,
which is an internal gRPC path not exposed as a stable public contract).

The adapter is fully async, built on raw ``httpx.AsyncClient`` (no
``msgraph-sdk``), and hits roughly the same error-mapping and telemetry
patterns as ``NextcloudBackend``.

See §5.2 of ``knowledge-base/knowledge/features/main_cloud_abstraction.md`` for the full
design, and §13.2 for the underlying research references.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Optional
from urllib.parse import quote

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
from orchestrator.services.cloud.config import OpenCloudSettings
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
from orchestrator.services.cloud.telemetry import instrument_backend_op

logger = logging.getLogger(__name__)

BACKEND_ID = "opencloud"

_DEFAULT_QUOTA_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
# OpenCloud ships several roles with identical displayName ("Can edit",
# "Can view") but different applicability — some work on whole Spaces,
# others on subfolders/files inside a Space. The `@libre.graph.weight`
# distinguishes them:
#
#   "Can view"   weight=10 (shares)  / weight=40 (Spaces)
#   "Can edit"   weight=60 (shares)  / weight=90 (Spaces) / weight=100 (files)
#   "Can manage" weight=120 (Spaces only)
#
# The Space-weighted roles (40 / 90 / 120) target the `/drives/{id}/root/invite`
# endpoint — i.e. granting access to a whole drive. The share-weighted roles
# (10 / 60) target `/drives/{id}/items/{item}/invite` — i.e. a specific
# subfolder within a drive. Using the wrong weight returns
# `400 "role not applicable to this resource"`.
#
# Session folders are subfolders inside the agent-home Space, so they invite
# with the share-weighted "Can edit" (weight=60). Project folders share the
# whole Space with a group, so they use the Space-weighted "Can edit"
# (weight=90).
#
# The well-known UUIDs are stable across OpenCloud releases (they come from
# the roleDefinitions compiled into the binary), but we still resolve by
# (displayName, weight) at runtime so forks that change the UUIDs keep
# working.
_SPACE_EDITOR_ROLE_WEIGHT = 90  # "Can edit" — whole-Space invite
_SPACE_VIEWER_ROLE_WEIGHT = 40  # "Can view" — whole-Space invite
_SPACE_EDITOR_ROLE_NAME = "Can edit"
_SPACE_VIEWER_ROLE_NAME = "Can view"
_FOLDER_EDITOR_ROLE_WEIGHT = 60  # "Can edit" — subfolder invite
_FOLDER_EDITOR_ROLE_NAME = "Can edit"
_AGENT_HOME_SPACE_NAME = "srw-agent-home"
_TOKEN_CLOCK_SKEW_SECONDS = 30.0

_NO_USER_READER = "no per-user read-only credential exists"

#: What OpenCloud delivers (main_cloud_as_connectors.md, "Provider support
#: matrix"). ``ensure_ro_reader``/``mint_ro_grant`` exist but nothing calls
#: them, so no read-only or protected level is offered.
OPENCLOUD_CAPABILITIES = ProviderCapabilities(
    backend_id=BACKEND_ID,
    title="OpenCloud",
    capabilities=(
        CloudCapability(
            "cloud_folder",
            "project",
            "read_only",
            "unsupported",
            "the read-only reader code exists, but nothing calls it yet",
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "read_write",
            "offered",
            "the orchestrator's Keycloak service account, a member of the "
            "project Space; the workspace only holds short-lived tokens",
            frozenset({"sandbox"}),
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "protected",
            "unsupported",
            "the protected review lane is built for Nextcloud only",
        ),
        CloudCapability(
            "cloud_folder",
            "user_root",
            "read_write",
            "unsupported",
            "works today only through deprecated Keycloak impersonation, "
            "so it is not offered",
        ),
        CloudCapability(
            "cloud_folder", "user_root", "read_only", "unsupported", _NO_USER_READER
        ),
        CloudCapability(
            "cloud_folder", "user_root", "protected", "unsupported", _NO_USER_READER
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


class OpenCloudBackend:
    """Main-cloud backend for OpenCloud.

    Uses Keycloak client_credentials to obtain a service-account access
    token, then calls LibreGraph on the OpenCloud instance. The token is
    cached and refreshed ~30s before expiry.

    The "agent home" Space hosts every session folder — analogous to
    Nextcloud's ``agent-service`` user home. It is created (if missing)
    at ``ensure_initialized`` time and its drive id is cached on the
    instance for the lifetime of the process.
    """

    backend_id = BACKEND_ID
    capabilities = OPENCLOUD_CAPABILITIES
    #: What ``ensure_initialized`` proves: it mints a service-account token
    #: from the issuer with the client credentials and reaches the internal
    #: URL with it. The public URL and the role, quota and TLS settings are
    #: never exercised.
    attested_fields: frozenset[str] = frozenset(
        {
            "version",
            "backend_id",
            "base_url",
            "keycloak_issuer",
            "keycloak_client_id",
            "keycloak_client_secret",
        }
    )

    def __init__(self, settings: OpenCloudSettings) -> None:
        self._settings = settings
        self._base_url = str(settings.base_url).rstrip("/")
        self._public_url = str(settings.public_url).rstrip("/")
        self._keycloak_issuer = str(settings.keycloak_issuer).rstrip("/")
        self._client_id = settings.keycloak_client_id
        self._client_secret = settings.keycloak_client_secret.get_secret_value()
        self._default_quota = (
            settings.default_quota_bytes
            if settings.default_quota_bytes is not None
            else _DEFAULT_QUOTA_BYTES
        )

        self._initialized = False
        self._client: Optional[httpx.AsyncClient] = None
        self._backend_instance_id: Optional[str] = None
        self._installation_proof_sha256: Optional[str] = None

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._token_lock = asyncio.Lock()

        # Keyed by (displayName, weight) — OpenCloud has duplicate role display
        # names distinguished only by @libre.graph.weight.
        self._role_cache: dict[tuple[str, int], str] = {}

        # Populated by ensure_initialized once the agent-home Space exists.
        self._agent_home_drive_id: Optional[str] = None
        self._agent_home_webdav_base: Optional[str] = None
        self._agent_home_web_url: Optional[str] = None

    # --------------------------------------------------------------- Properties

    @property
    def is_configured(self) -> bool:
        return bool(
            self._base_url
            and self._keycloak_issuer
            and self._client_id
            and self._client_secret
        )

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def backend_instance_id(self) -> Optional[str]:
        return self._backend_instance_id

    @property
    def installation_proof_sha256(self) -> Optional[str]:
        return self._installation_proof_sha256

    def bind_backend_instance(self, backend_instance_id: str) -> None:
        """Bind the DB-adopted installation UUID exactly once."""

        if self._backend_instance_id not in {None, backend_instance_id}:
            raise RuntimeError("OpenCloud backend instance authority changed")
        self._backend_instance_id = backend_instance_id

    def prepare_backend_instance_attestation(self, backend_instance_id: str) -> None:
        """OpenCloud's provider proof does not depend on the proposed UUID."""

        if not isinstance(backend_instance_id, str) or not backend_instance_id:
            raise ValueError("OpenCloud attestation instance is missing")

    @property
    def webdav_credentials(self) -> dict[str, str]:
        """OpenCloud WebDAV is OAuth bearer-based, not user+password.

        Agents cannot re-use a username/password pair here — they need a
        live bearer token. The ``WorkspaceSyncService`` will grow a
        bearer-aware variant when the first customer wires OpenCloud up
        as their agent data channel; until then, return ``{}`` so the
        legacy ``username``/``password`` plumbing silently falls back.
        """
        return {}

    def cloud_sync_config(
        self, webdav_url: str, *, target_user_sub: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        """The agent's ``cloud_sync`` entry for one folder, or ``None``.

        The agent mints service-account tokens via client credentials. With
        ``target_user_sub`` (a user-home Space, owned by exactly one user and
        invisible to the service account) it exchanges that token for a
        user-scoped one (RFC 8693) and authenticates WebDAV with it. Never
        logs the auth payload.
        """
        if not self._client_secret:
            return None
        auth: dict[str, Any] = {
            "issuer": self._keycloak_issuer,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if target_user_sub:
            auth["type"] = "keycloak_user_impersonation"
            auth["target_user_sub"] = target_user_sub
        else:
            auth["type"] = "keycloak_client_credentials"
        return {"backend": self.backend_id, "webdav_url": webdav_url, "auth": auth}

    @staticmethod
    def legacy_folder_id(handle: ProjectFolderHandle) -> Optional[int]:
        """OpenCloud never had a ``projects.nextcloud_folder_id``."""
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
        """Build an rclone WebDAV spec for an OpenCloud Space surface.

        OpenCloud WebDAV is OAuth-bearer-only, so the spec carries no static
        credential. ``auth.type = "keycloak_client_credentials"`` tells the
        agent-side mount manager to mint service-account tokens through the
        shared ``KeycloakTokenClient`` and feed them to rclone via a
        runtime-local ``bearer_token_command`` helper — the workspace host
        only ever sees the short-lived access token, never the client
        secret. See knowledge-base/knowledge/features/rclone_cloud_mount.md §11.

        User-home mounts (Personal Spaces) use
        ``auth.type = "keycloak_user_impersonation"``: the agent exchanges
        its service token for a user-scoped token via RFC 8693
        (``requested_subject``), because a Personal Space is owned by
        exactly one user and the service account has no WebDAV access to
        it. Requires the realm-management ``impersonation`` role on this
        client's service account (granted by the bundled Keycloak setup).
        Without a resolvable target sub they raise ``NOT_SUPPORTED`` so the
        thread takes the documented session-folder fallback.
        """
        self._ensure_ready()
        auth: dict[str, Any] = {
            "type": "keycloak_client_credentials",
            "issuer": self._keycloak_issuer,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if isinstance(handle, SessionFolderHandle):
            webdav_url = self.get_session_folder_webdav_url(handle)
        else:
            # Always reconstruct from the internal base URL. The persisted
            # vendor_meta webdav_url comes from the graph create/lookup
            # response and is the PUBLIC URL — mounting that would hairpin
            # all rclone traffic through the public edge (and fails on
            # local k3d, where workspace pods cannot reach it at all).
            # handle.native_id is the Space drive id for both project and
            # user-home handles.
            webdav_url = (
                f"{self._base_url}/dav/spaces/{quote(handle.native_id, safe='')}/"
                if handle.native_id
                else None
            )
            if handle.vendor_meta.get("kind") == "user_home":
                target_user_sub = (subject.user_sub if subject else None) or ""
                if not target_user_sub:
                    raise CloudBackendError(
                        CloudBackendErrorKind.NOT_SUPPORTED,
                        f"cannot build rclone mount for {mount_kind}: "
                        "user-home mount has no target user sub for "
                        "impersonation; session-folder fallback applies",
                        backend=self.backend_id,
                    )
                auth["type"] = "keycloak_user_impersonation"
                auth["target_user_sub"] = target_user_sub

        # Cross-cluster VM runtimes can't reach the internal service URL
        # (srw-opencloud:9200) — swap to the public edge, which the VM egress
        # NetworkPolicy permits (443). Same-cluster pods keep the internal URL
        # (no hairpin, works on local k3d). See
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

        return RcloneMountSpec(
            source_type="webdav",
            source_config={
                "url": webdav_url,
                # OCIS-lineage vendor: tus chunked uploads + OpenCloud
                # quirks. Exists from rclone 1.70.0 — enforced below.
                "vendor": "infinitescale",
            },
            auth=auth,
            # Local-dev knob (OPENCLOUD_MOUNT_INSECURE_TLS): the tus PATCH
            # hop targets the PUBLIC data-gateway URL, which on local k3d
            # presents an mkcert cert the workspace image doesn't trust.
            provider_flags=(
                ["--no-check-certificate"] if self._settings.mount_insecure_tls else []
            ),
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
            required_capabilities=["rclone", "fuse", "rc", "token_helper"],
            min_rclone_version="1.70.0",
        )

    # ---------------------------------------------------------------- Lifecycle

    async def ensure_initialized(self) -> bool:
        """Verify reachability, fetch role catalog, ensure agent-home Space.

        Soft-failure semantics: returns ``False`` when OpenCloud is not
        configured or unreachable so the rest of the orchestrator can
        boot. After this, every other method raises ``CloudBackendError``
        on hard failure.
        """
        if not self.is_configured:
            logger.info(
                "OpenCloud backend not configured "
                "(missing base_url / keycloak_issuer / client_id / client_secret)"
            )
            return False

        try:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={
                    "User-Agent": "SuperhumanRemoteWorker/0.1 main-cloud-backend",
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=10.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )

            # Sanity: fetch a token. Any failure here surfaces config problems
            # (wrong Keycloak URL, bad secret, disabled service account) at
            # startup instead of during the first tenant call.
            await self._get_service_token()

            # Fetch the role catalog so sharing calls don't race on the first
            # invite. Non-fatal if the endpoint 404s on older servers — we
            # fall back to lazy resolution.
            try:
                self._role_cache = await self._load_role_catalog()
            except CloudBackendError as e:
                logger.warning(
                    "OpenCloud role catalog preload failed (%s) — "
                    "adapter will retry lazily on first invite.",
                    e,
                )

            # Ensure the agent-home Space exists — this is where session
            # folders live. Cache its drive id for the process lifetime.
            (
                self._agent_home_drive_id,
                self._agent_home_web_url,
            ) = await self._ensure_agent_home_space()
            self._agent_home_webdav_base = (
                f"{self._base_url}/dav/spaces/{self._agent_home_drive_id}"
            )
            self._installation_proof_sha256 = main_cloud_installation_proof_sha256(
                backend_id=self.backend_id,
                remote_identity=str(self._agent_home_drive_id),
            )

            self._initialized = True
            logger.info(
                "OpenCloud backend initialized (agent-home drive_id=%s)",
                self._agent_home_drive_id,
            )
            return True
        except Exception as e:
            logger.warning("OpenCloud backend init failed: %s", e)
            if self._client is not None:
                await self._client.aclose()
                self._client = None
            return False

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None
        self._initialized = False
        self._access_token = None
        self._token_expires_at = 0.0

    async def health_check(self) -> HealthStatus:
        if not self._initialized or self._client is None:
            return HealthStatus(ok=False, latency_ms=0.0, detail="not initialized")
        try:
            start = time.monotonic()
            await self._graph_get("/graph/v1.0/me")
            elapsed_ms = (time.monotonic() - start) * 1000
            return HealthStatus(ok=True, latency_ms=elapsed_ms, detail="ok")
        except Exception as e:
            return HealthStatus(ok=False, latency_ms=0.0, detail=str(e))

    # ------------------------------------------------------------------ Identity

    @instrument_backend_op("resolve_user_identity")
    async def resolve_user_identity(
        self, email: Optional[str], display_name: Optional[str]
    ) -> Optional[UserId]:
        """Resolve an SSO user to the backend's LibreGraph user id.

        LibreGraph does NOT support ``$filter=mail eq '...'`` on /users —
        only ``memberOf/any(...)`` and ``appRoleAssignments/any(...)``
        are honored. We use ``$search`` instead and do a client-side
        exact match on the ``mail`` / ``displayName`` fields.
        """
        self._ensure_ready()
        candidates = [c for c in (email, display_name) if c]
        if not candidates:
            return None
        try:
            for query in candidates:
                resp = await self._graph_get(
                    "/graph/v1.0/users",
                    params={"$search": f'"{query}"'},
                )
                users = resp.json().get("value", [])
                if email:
                    for user in users:
                        if (user.get("mail") or "").lower() == email.lower():
                            return UserId(str(user["id"]))
                if display_name:
                    for user in users:
                        if (
                            user.get("displayName") or ""
                        ).lower() == display_name.lower():
                            return UserId(str(user["id"]))
                if len(users) == 1:
                    return UserId(str(users[0]["id"]))
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        return None

    @instrument_backend_op("ensure_user")
    async def ensure_user(
        self,
        *,
        sub: str,
        issuer: str,
        email: Optional[str],
        display_name: Optional[str],
        preferred_username: Optional[str] = None,
    ) -> Optional[UserId]:
        """Create the OpenCloud user record for an SSO identity if missing.

        Without this, OpenCloud only provisions a user on their first
        browser login, which races session-folder sharing for new threads.

        Looks up by (issuer, sub) first — that's the stable identity — then
        falls back to email/displayName resolution. POSTs a LibreGraph user
        with ``identities[]`` carrying the Keycloak ``sub`` so the existing
        autoprovision path still reconciles on login.
        """
        self._ensure_ready()
        existing = await self._find_user_by_sub(issuer, sub)
        if existing:
            return existing
        fallback = await self.resolve_user_identity(email, display_name)
        if fallback:
            return fallback
        if not email or not display_name:
            return None
        # userType is read-only in OpenCloud's LibreGraph impl — including it
        # yields 400 "userType is a read-only attribute". Defaults to Member.
        body: dict[str, Any] = {
            "accountEnabled": True,
            "displayName": display_name,
            "mail": email,
            "identities": [
                {"issuer": issuer, "issuerAssignedId": sub},
            ],
        }
        if preferred_username:
            body["onPremisesSamAccountName"] = preferred_username
        try:
            resp = await self._graph_post("/graph/v1.0/users", json=body)
            # OpenCloud returns 400 with an error code of "nameAlreadyExists" on
            # concurrent-create collisions (not the 409 LibreGraph docs imply).
            # Treat both as a race — look up the user that the other caller
            # already created instead of failing the whole provision.
            if resp.status_code == 409 or (
                resp.status_code == 400 and "nameAlreadyExists" in resp.text
            ):
                return await self._find_user_by_sub(
                    issuer, sub
                ) or await self.resolve_user_identity(email, display_name)
            resp.raise_for_status()
            user_id = resp.json().get("id")
            if user_id:
                logger.info(
                    "Provisioned OpenCloud user %s (sub=%s, email=%s)",
                    user_id,
                    sub,
                    email,
                )
                return UserId(str(user_id))
            return None
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    async def _find_user_by_sub(self, issuer: str, sub: str) -> Optional[UserId]:
        """Locate an existing LibreGraph user by (issuer, sub) identity tuple."""
        try:
            resp = await self._graph_get(
                "/graph/v1.0/users",
                params={"$search": f'"{sub}"'},
            )
            for user in resp.json().get("value", []):
                for identity in user.get("identities") or []:
                    if (
                        identity.get("issuer") == issuer
                        and identity.get("issuerAssignedId") == sub
                    ):
                        return UserId(str(user["id"]))
        except httpx.HTTPError:
            return None
        return None

    def get_default_home_browser_url(self) -> Optional[str]:
        """Generic OpenCloud Files app URL (user-agnostic)."""
        return f"{self._public_url}/files/"

    async def get_user_home(self, user_id: UserId) -> Optional[UserHome]:
        """Fetch the user's personal Space and return its URL triple."""
        if not self._initialized or self._client is None:
            return None
        try:
            resp = await self._graph_get(
                f"/graph/v1.0/users/{user_id}",
                params={"$expand": "drive"},
            )
            user_data = resp.json()
            drive = user_data.get("drive") or {}
            drive_id = drive.get("id")
            if not drive_id:
                return None
            browser_url = (drive.get("webUrl") or f"{self._public_url}/files/").rstrip(
                "/"
            )
            root = drive.get("root") or {}
            webdav_url = root.get("webDavUrl") or (
                f"{self._base_url}/dav/spaces/{drive_id}/"
            )
            handle = ProjectFolderHandle(
                backend=self.backend_id,
                native_id=str(drive_id),
                vendor_meta={
                    "kind": "user_home",
                    "username": str(user_id),
                    "webdav_url": webdav_url,
                },
            )
            return UserHome(
                handle=handle,
                browser_url=browser_url,
                webdav_url=webdav_url,
            )
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    # -------------------------------------------------------------------- Groups

    @instrument_backend_op("ensure_group")
    async def ensure_group(self, group_id: GroupId) -> None:
        """Create a LibreGraph group if missing (idempotent).

        Persists the requested ``group_id`` as ``displayName``. The server
        assigns a UUID; callers never see it because every subsequent
        operation looks the group up by displayName via
        ``_resolve_group_id``.
        """
        self._ensure_ready()
        try:
            existing = await self._resolve_group_id(group_id)
            if existing is not None:
                return
            resp = await self._graph_post(
                "/graph/v1.0/groups",
                json={"displayName": str(group_id)},
            )
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("add_user_to_group")
    async def add_user_to_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        try:
            resolved = await self._require_group_id(group_id)
            resp = await self._graph_post(
                f"/graph/v1.0/groups/{resolved}/members/$ref",
                json={
                    "@odata.id": (f"{self._base_url}/graph/v1.0/users/{user_id}"),
                },
            )
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("remove_user_from_group")
    async def remove_user_from_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        try:
            resolved = await self._require_group_id(group_id)
            resp = await self._graph_delete(
                f"/graph/v1.0/groups/{resolved}/members/{user_id}/$ref",
            )
            if resp.status_code == 404:
                return  # already gone — idempotent
            resp.raise_for_status()
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    # ------------------------------------------------------------- Project folders

    @instrument_backend_op("ensure_project_folder")
    async def ensure_project_folder(
        self, *, project_name: str, group_id: GroupId
    ) -> ProjectFolderHandle:
        """Create a project Space and grant the group editor access.

        LibreGraph:
          1. ``POST /graph/v1.0/drives`` creates the Space
             (driveType is auto-assigned as ``project``).
          2. ``POST /graph/v1beta1/drives/{id}/root/invite`` grants the
             project group the Space-applicable "Can edit" role.
        """
        self._ensure_ready()
        try:
            quota: dict[str, Any] = {}
            if self._default_quota:
                quota = {"quota": {"total": self._default_quota}}
            resp = await self._graph_post(
                "/graph/v1.0/drives",
                json={"name": project_name, **quota},
            )
            resp.raise_for_status()
            drive = resp.json()
            drive_id = drive.get("id")
            if not drive_id:
                raise CloudBackendError(
                    CloudBackendErrorKind.UNKNOWN,
                    "OpenCloud drive creation returned no id",
                    backend=self.backend_id,
                    raw=drive,
                )
            root = drive.get("root") or {}
            web_url = (drive.get("webUrl") or "").rstrip("/")
            webdav_url = root.get("webDavUrl") or (
                f"{self._base_url}/dav/spaces/{drive_id}/"
            )

            # Grant the project group editor access on the new Space.
            group_native_id = await self._require_group_id(group_id)
            await self._invite_group(
                drive_id,
                group_native_id,
                _SPACE_EDITOR_ROLE_NAME,
                _SPACE_EDITOR_ROLE_WEIGHT,
            )

            logger.info(
                "Created OpenCloud Space '%s' (drive_id=%s) for group '%s'",
                project_name,
                drive_id,
                group_id,
            )
            return ProjectFolderHandle(
                backend=self.backend_id,
                native_id=str(drive_id),
                vendor_meta={
                    "mountpoint": project_name,
                    "webdav_url": webdav_url,
                    "web_url": web_url,
                },
            )
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("delete_project_folder")
    async def delete_project_folder(
        self, handle: ProjectFolderHandle, *, if_exists: bool = True
    ) -> None:
        """Disable-then-purge delete pattern.

        OpenCloud refuses to purge an enabled Space with a 400. The web UI
        works around this by first sending a DELETE with no headers (which
        disables the Space) and then a DELETE with ``Purge: T`` (which
        permanently removes it). This is not documented upstream and was
        discovered from Web UI traffic; see §5.2 of the design doc.
        """
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "missing drive id on ProjectFolderHandle",
                backend=self.backend_id,
            )
        try:
            # Step 1: disable the Space.
            resp = await self._graph_delete(f"/graph/v1.0/drives/{drive_id}")
            if resp.status_code == 404:
                if if_exists:
                    logger.debug("OpenCloud Space drive_id=%s already gone", drive_id)
                    return
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_FOUND,
                    f"Space {drive_id} not found",
                    backend=self.backend_id,
                    status_code=404,
                )
            # 204 No Content on disable, some versions return 200.
            if resp.status_code not in (200, 204):
                resp.raise_for_status()

            # Step 2: purge the disabled Space.
            resp = await self._graph_delete(
                f"/graph/v1.0/drives/{drive_id}",
                extra_headers={"Purge": "T"},
            )
            if resp.status_code == 404:
                # Purge after disable can return 404 when the server has
                # already reaped it; treat as success.
                return
            resp.raise_for_status()
            logger.info("Purged OpenCloud Space drive_id=%s", drive_id)
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("refresh_project_folder_access")
    async def refresh_project_folder_access(
        self, handle: ProjectFolderHandle, group_id: GroupId
    ) -> None:
        """No-op for OpenCloud.

        Unlike Nextcloud (server#57445), OpenCloud propagates group
        membership changes to Space grants automatically. The method is
        kept in the Protocol for symmetry and is a silent no-op on this
        backend; operators never need to poke the Space to refresh
        access for a newly-added member.
        """
        self._ensure_ready()
        _ = handle, group_id  # documented no-op — see docstring
        return

    def get_project_folder_browser_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        """Browser deep-link to a Space in the OpenCloud Web UI.

        Derived from the LIVE ``public_url`` plus the Space's stable fileId
        (``native_id``) via OpenCloud's ``/f/<fileId>`` private-link
        resolver — the same form ``get_session_folder_browser_url`` uses.
        The resolver routes to the exact Space for any member, so we do NOT
        need the drive *alias*; and we deliberately do NOT return the
        persisted ``vendor_meta.web_url``. That value is the ``webUrl``
        OpenCloud emitted at Space-creation time, which bakes in whatever
        ``OC_URL`` was canonical then — a later domain rebrand
        (``cloud.superhuman-remote-worker.com`` -> ``cloud.srw.works``)
        freezes every stored link on the dead host. Reconstructing keeps the
        link current for old and new projects alike.
        """
        drive_id = handle.native_id
        if drive_id:
            # Delimiters in the composite fileId must be percent-encoded.
            # See opencloud-eu/web#1795 — most HTTP clients skip `!` and `$`
            # by default.
            safe_id = quote(drive_id, safe="")
            return f"{self._public_url}/f/{safe_id}"
        # Legacy handle with no drive id: nothing to reconstruct from, so fall
        # back to whatever absolute link was captured (may carry a stale host).
        web_url = handle.vendor_meta.get("web_url")
        return str(web_url) if web_url else None

    @instrument_backend_op("list_project_folder")
    async def list_project_folder(
        self,
        handle: ProjectFolderHandle,
        *,
        subpath: str = "",
    ) -> list[ProjectFolderEntry]:
        """Recursive enumeration of a project folder Space, via repeated
        ``PROPFIND`` with ``Depth: 1``.

        OpenCloud (oCIS) rejects ``Depth: infinity`` with HTTP 400, so we
        walk the tree breadth-first instead — one ``Depth: 1`` PROPFIND
        per directory. Returns every descendant (files + directories);
        order is breadth-first.
        """
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "list_project_folder: handle has no drive id",
                backend=self.backend_id,
                retryable=False,
            )
        safe_drive = quote(drive_id, safe="")
        base_path = f"/dav/spaces/{safe_drive}"
        # Strip the leading prefix off of every href so callers get
        # paths relative to the project root.
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

        Sent with no body — the server returns all known props by
        default, same shape that ``_resolve_item_id`` (Depth=0) gets
        back. Including a body XML had OpenCloud returning 400 in
        testing.
        """
        token = await self._get_service_token()
        try:
            resp = await self._client.request(
                "PROPFIND",
                url_path,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Depth": "1",
                },
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
        """PROPFIND at an explicit Depth. OpenCloud (oCIS) rejects
        ``Depth: infinity`` with 400, which raises ``PropfindError`` so the
        etag baseline falls straight through to a Depth:1 BFS (design §11.5)."""
        token = await self._get_service_token()
        try:
            resp = await self._client.request(
                "PROPFIND",
                url_path,
                headers={"Authorization": f"Bearer {token}", "Depth": depth},
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
        """Mount-time ``{path: etag}`` baseline for the conflict gate. OpenCloud
        rejects ``Depth: infinity``, so this always walks Depth:1 BFS via the
        shared helper's fallback path (design §3.4, §11.5)."""
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "capture_etag_baseline: handle has no drive id",
                backend=self.backend_id,
                retryable=False,
            )
        safe_drive = quote(drive_id, safe="")
        base_path = f"/dav/spaces/{safe_drive}"
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
        """GET one file out of a project folder Space."""
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "get_project_folder_file_bytes: handle has no drive id",
                backend=self.backend_id,
                retryable=False,
            )
        safe_drive = quote(drive_id, safe="")
        rel = path.lstrip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "get_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        safe_path = quote(rel, safe="/")
        url = f"/dav/spaces/{safe_drive}/{safe_path}"
        token = await self._get_service_token()
        try:
            resp = await self._client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        if resp.status_code == 404:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"file {path!r} not found in drive {drive_id}",
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
        """PUT one file into a project folder Space, MKCOL-ing parents.

        Same MKCOL-each-segment-then-PUT idiom as ``put_session_file``.
        Used by the Mode A accept flow (job_cloud_export.md §3.5) to
        write the agent's edits back to the cloud folder.
        """
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_project_folder_file_bytes: handle has no drive id",
                backend=self.backend_id,
                retryable=False,
            )
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        safe_drive = quote(drive_id, safe="")
        base = f"/dav/spaces/{safe_drive}"
        token = await self._get_service_token()
        segments = rel.split("/")
        parents = segments[:-1]
        try:
            for i in range(len(parents)):
                partial = "/".join(parents[: i + 1])
                url = f"{base}/{quote(partial, safe='/')}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()

            put_url = f"{base}/{quote(rel, safe='/')}"
            headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
            if content_type:
                headers["Content-Type"] = content_type
            put_resp = await self._client.put(put_url, headers=headers, content=content)
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
        """DELETE one file from a project folder Space."""
        self._ensure_ready()
        drive_id = handle.native_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "delete_project_folder_file: handle has no drive id",
                backend=self.backend_id,
                retryable=False,
            )
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "delete_project_folder_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        safe_drive = quote(drive_id, safe="")
        url = f"/dav/spaces/{safe_drive}/{quote(rel, safe='/')}"
        token = await self._get_service_token()
        try:
            resp = await self._client.delete(
                url,
                headers={"Authorization": f"Bearer {token}"},
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
                f"file {path!r} not found in drive {drive_id}",
                backend=self.backend_id,
                status_code=404,
            )
        raise CloudBackendError(
            CloudBackendErrorKind.UNKNOWN,
            f"DELETE file {path!r}: unexpected status {resp.status_code}",
            backend=self.backend_id,
            status_code=resp.status_code,
        )

    def get_project_folder_webdav_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        """Internal WebDAV URL for a Space.

        Reconstructed from the LIVE internal ``base_url`` plus the Space drive
        id, mirroring ``build_rclone_mount_spec`` (which already reconstructs
        rather than trusting ``vendor_meta``). The persisted ``webdav_url`` is
        the PUBLIC graph-response URL and freezes on the ``OC_URL`` domain at
        creation time, so trusting it both hairpins traffic through the edge
        and breaks after a domain rebrand. Only fall back to the persisted
        value for legacy handles that carry no drive id.
        """
        drive_id = handle.native_id
        if drive_id:
            safe_id = quote(drive_id, safe="")
            return f"{self._base_url}/dav/spaces/{safe_id}/"
        persisted = handle.vendor_meta.get("webdav_url")
        return str(persisted) if persisted else None

    # ------------------------------------------------------------- Session folders

    @instrument_backend_op("ensure_session_folder")
    async def ensure_session_folder(self, *, session_id: str) -> SessionFolderHandle:
        """Create ``sessions/{session_id}`` under the agent-home Space via WebDAV.

        OpenCloud WebDAV lives at ``{base_url}/dav/spaces/{drive-id}/``
        (note: NOT ``/remote.php/dav/...`` — that's Nextcloud only). The
        path must be MKCOL-ed one segment at a time because intermediate
        collections are NOT auto-created.
        """
        self._ensure_ready()
        if self._agent_home_webdav_base is None:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "agent-home Space WebDAV base not set",
                backend=self.backend_id,
            )
        folder_path = f"sessions/{session_id}"
        parts = folder_path.strip("/").split("/")
        token = await self._get_service_token()
        try:
            for i in range(len(parts)):
                partial = "/".join(parts[: i + 1])
                url = f"{self._agent_home_webdav_base}/{partial}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()
            logger.info("Created OpenCloud session folder: %s", folder_path)
            vendor_meta: dict[str, Any] = {"drive_id": self._agent_home_drive_id}
            try:
                # Capture the compound fileId so the cloud-button URL can
                # deep-link via OpenCloud's /f/<fileId> private-link resolver.
                # PROPFIND failure is non-fatal — URL falls back to Shares view.
                vendor_meta["item_id"] = await self._resolve_item_id(
                    self._agent_home_drive_id, folder_path
                )
            except Exception as e:
                logger.warning(
                    "Could not resolve item_id for new session folder %s: %s",
                    folder_path,
                    e,
                )
            return SessionFolderHandle(
                backend=self.backend_id,
                native_id=folder_path,
                vendor_meta=vendor_meta,
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("delete_session_folder")
    async def delete_session_folder(
        self, handle: SessionFolderHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        if self._agent_home_webdav_base is None:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "agent-home Space WebDAV base not set",
                backend=self.backend_id,
            )
        folder_path = handle.native_id
        token = await self._get_service_token()
        try:
            url = f"{self._agent_home_webdav_base}/{folder_path}"
            resp = await self._client.delete(
                url,
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code in (200, 204):
                logger.info("Deleted OpenCloud session folder: %s", folder_path)
                return
            if resp.status_code == 404:
                if if_exists:
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
        """Invite a user to a session folder via LibreGraph sharing v1beta1."""
        self._ensure_ready()
        drive_id = handle.vendor_meta.get("drive_id") or self._agent_home_drive_id
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "session handle is missing drive_id and no agent-home cached",
                backend=self.backend_id,
            )
        folder_path = handle.native_id
        try:
            role_id = await self._role_id(
                _FOLDER_EDITOR_ROLE_NAME,
                _FOLDER_EDITOR_ROLE_WEIGHT,
            )
            item_id = await self._resolve_item_id(str(drive_id), folder_path)
            safe_drive = quote(str(drive_id), safe="")
            safe_item = quote(item_id, safe="")
            resp = await self._graph_post(
                f"/graph/v1beta1/drives/{safe_drive}/items/{safe_item}/invite",
                json={
                    "recipients": [
                        {
                            "@libre.graph.recipient.type": "user",
                            "objectId": str(user_id),
                        }
                    ],
                    "roles": [role_id],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            perms = data.get("value") or []
            if not perms:
                raise CloudBackendError(
                    CloudBackendErrorKind.UNKNOWN,
                    "OpenCloud invite returned no permissions",
                    backend=self.backend_id,
                    raw=data,
                )
            permission_id = perms[0].get("id")
            if not permission_id:
                raise CloudBackendError(
                    CloudBackendErrorKind.UNKNOWN,
                    "OpenCloud invite permission missing id",
                    backend=self.backend_id,
                    raw=data,
                )
            return ShareHandle(
                backend=self.backend_id,
                native_id=str(permission_id),
                vendor_meta={
                    "drive_id": str(drive_id),
                    "item_id": item_id,
                },
            )
        except CloudBackendError:
            raise
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    @instrument_backend_op("revoke_session_share")
    async def revoke_session_share(
        self, share: ShareHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        drive_id = share.vendor_meta.get("drive_id")
        item_id = share.vendor_meta.get("item_id")
        if not drive_id or not item_id:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "ShareHandle.vendor_meta missing drive_id/item_id",
                backend=self.backend_id,
            )
        safe_drive = quote(str(drive_id), safe="")
        safe_item = quote(str(item_id), safe="")
        safe_permission = quote(share.native_id, safe="")
        try:
            resp = await self._graph_delete(
                f"/graph/v1beta1/drives/{safe_drive}/items/{safe_item}"
                f"/permissions/{safe_permission}",
            )
            if resp.status_code in (200, 204):
                return
            if resp.status_code == 404:
                if if_exists:
                    return
                raise CloudBackendError(
                    CloudBackendErrorKind.NOT_FOUND,
                    f"Share {share.native_id} not found",
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
        """Best-effort browser link to the session folder for the thread owner.

        Session folders live in the orchestrator's ``srw-agent-home`` Space
        and are shared with the owner via a folder-level invite. Deep-linking
        into that Space 404s for the recipient because they are not a Space
        member. OpenCloud's ``/f/<fileId>`` private-link resolver handles this
        by dispatching on the viewer's effective access — a share recipient
        gets routed to their Shares-mounted view of the same folder. Fall
        back to the "Shared with me" list if the item_id wasn't captured
        (older sessions created before we started storing it).
        """
        item_id = handle.vendor_meta.get("item_id")
        if item_id:
            safe_id = quote(str(item_id), safe="")
            return f"{self._public_url}/f/{safe_id}"
        return f"{self._public_url}/files/shares/with-me"

    def get_session_folder_webdav_url(
        self, handle: SessionFolderHandle
    ) -> Optional[str]:
        drive_id = handle.vendor_meta.get("drive_id") or self._agent_home_drive_id
        if not drive_id:
            return None
        safe_id = quote(str(drive_id), safe="")
        safe_path = quote(handle.native_id, safe="/")
        return f"{self._base_url}/dav/spaces/{safe_id}/{safe_path}/"

    @instrument_backend_op("put_session_file")
    async def put_session_file(
        self,
        handle: SessionFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> None:
        """PUT one file into a session folder, MKCOL-ing parents on the way.

        ``path`` is slash-separated, relative to the session folder root,
        no leading slash. WebDAV PUT does not auto-create missing
        intermediate collections, so we MKCOL each parent segment one
        at a time first (idempotent — 405 means "already there").
        """
        self._ensure_ready()
        if self._agent_home_webdav_base is None:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "agent-home Space WebDAV base not set",
                backend=self.backend_id,
            )
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_session_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        token = await self._get_service_token()
        folder_path = handle.native_id.strip("/")
        full_segments = folder_path.split("/") + rel.split("/")
        # MKCOL every parent collection of the target file.
        parents = full_segments[:-1]
        try:
            for i in range(len(parents)):
                partial = "/".join(parents[: i + 1])
                url = f"{self._agent_home_webdav_base}/{quote(partial, safe='/')}"
                resp = await self._client.request(
                    "MKCOL",
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if resp.status_code in (201, 405):
                    continue
                resp.raise_for_status()

            put_url = (
                f"{self._agent_home_webdav_base}/"
                f"{quote('/'.join(full_segments), safe='/')}"
            )
            headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
            if content_type:
                headers["Content-Type"] = content_type
            put_resp = await self._client.put(put_url, headers=headers, content=content)
            if put_resp.status_code not in (200, 201, 204):
                put_resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    # ------------------------------------------------------------ Internal helpers

    def _ensure_ready(self) -> None:
        if not self._initialized or self._client is None:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "OpenCloud backend not initialized",
                backend=self.backend_id,
                retryable=False,
            )

    async def _get_service_token(self) -> str:
        """Return a cached or freshly-minted Keycloak service-account token.

        Refreshes ~30s before expiry to absorb clock skew between the
        orchestrator and Keycloak. Protected by ``_token_lock`` so
        concurrent callers share a single refresh.
        """
        now = time.monotonic()
        if self._access_token and now < self._token_expires_at:
            return self._access_token
        async with self._token_lock:
            now = time.monotonic()
            if self._access_token and now < self._token_expires_at:
                return self._access_token
            assert self._client is not None
            try:
                resp = await self._client.post(
                    f"{self._keycloak_issuer}/protocol/openid-connect/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self._client_id,
                        "client_secret": self._client_secret,
                        # openid scope is required so the token unlocks the
                        # Keycloak userinfo endpoint that OpenCloud's proxy
                        # calls as part of its OIDC middleware. Without it,
                        # OpenCloud returns 401 on every API call with
                        # "failed to get userinfo: 403 Forbidden".
                        "scope": "openid",
                    },
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
            except httpx.HTTPError as e:
                raise self._map_http_error(e) from e
            payload = resp.json()
            token = payload.get("access_token")
            expires_in = payload.get("expires_in")
            if not token or not isinstance(expires_in, (int, float)):
                raise CloudBackendError(
                    CloudBackendErrorKind.AUTHENTICATION_FAILED,
                    "Keycloak token response missing access_token/expires_in",
                    backend=self.backend_id,
                    raw=payload,
                )
            self._access_token = str(token)
            self._token_expires_at = (
                time.monotonic() + float(expires_in) - _TOKEN_CLOCK_SKEW_SECONDS
            )
            return self._access_token

    async def _graph_get(
        self, path: str, *, params: Optional[dict[str, Any]] = None
    ) -> httpx.Response:
        assert self._client is not None
        token = await self._get_service_token()
        resp = await self._client.get(
            path,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp

    async def _graph_post(
        self,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
    ) -> httpx.Response:
        assert self._client is not None
        token = await self._get_service_token()
        return await self._client.post(
            path,
            json=json,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

    async def _graph_delete(
        self,
        path: str,
        *,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        assert self._client is not None
        token = await self._get_service_token()
        headers = {"Authorization": f"Bearer {token}"}
        if extra_headers:
            headers.update(extra_headers)
        return await self._client.delete(path, headers=headers)

    def _map_http_error(self, exc: Exception) -> CloudBackendError:
        """Translate ``httpx`` errors into ``CloudBackendError``."""
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
            request_id = resp.headers.get("x-request-id")
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

    # ---------------------------------------------------------------- Role catalog

    async def _load_role_catalog(self) -> dict[tuple[str, int], str]:
        """Fetch the role definitions from LibreGraph.

        Returns ``{(displayName, weight): roleId}``.  Keys are compound
        because OpenCloud ships several roles with identical displayName
        (e.g. multiple "Can edit" entries) distinguished only by their
        ``@libre.graph.weight``.

        Raises on 404/5xx so that callers can decide whether to fall back
        (``ensure_initialized`` logs and continues; ``_role_id`` refreshes
        and retries).

        OpenCloud returns this endpoint as a bare JSON array, unlike most
        other LibreGraph endpoints which wrap results in ``{"value": [...]}``.
        We accept both shapes so this adapter keeps working if the server
        wraps the payload in a future release.
        """
        resp = await self._graph_get(
            "/graph/v1beta1/roleManagement/permissions/roleDefinitions"
        )
        body = resp.json()
        if isinstance(body, list):
            roles = body
        elif isinstance(body, dict):
            roles = body.get("value", [])
        else:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                f"unexpected roleDefinitions response shape: {type(body).__name__}",
                backend=self.backend_id,
            )
        return {
            (role["displayName"], int(role.get("@libre.graph.weight", 0))): role["id"]
            for role in roles
            if "id" in role
        }

    async def _role_id(self, display_name: str, weight: int) -> str:
        """Return the role UUID for ``(display_name, weight)``, refreshing the cache if needed."""
        key = (display_name, weight)
        if key in self._role_cache:
            return self._role_cache[key]
        try:
            self._role_cache = await self._load_role_catalog()
        except CloudBackendError as e:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED,
                f"could not load OpenCloud role catalog: {e}",
                backend=self.backend_id,
            ) from e
        if key not in self._role_cache:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_SUPPORTED,
                f"role {display_name!r} not in OpenCloud role catalog",
                backend=self.backend_id,
            )
        return self._role_cache[key]

    # ---------------------------------------------------------------- Group lookup

    async def _resolve_group_id(self, group_id: GroupId) -> Optional[str]:
        """Look up a group by displayName. Returns the LibreGraph UUID or None."""
        resp = await self._graph_get(
            "/graph/v1.0/groups",
            params={"$search": f'"{group_id}"'},
        )
        for group in resp.json().get("value", []):
            if group.get("displayName") == str(group_id):
                return str(group.get("id"))
        return None

    async def _require_group_id(self, group_id: GroupId) -> str:
        resolved = await self._resolve_group_id(group_id)
        if resolved is None:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"group {group_id!r} not found in OpenCloud",
                backend=self.backend_id,
            )
        return resolved

    # ---------------------------------------------------------------- Invite helper

    async def _invite_group(
        self,
        drive_id: str,
        group_native_id: str,
        role_display_name: str,
        role_weight: int,
    ) -> None:
        role_id = await self._role_id(role_display_name, role_weight)
        safe_drive = quote(drive_id, safe="")
        resp = await self._graph_post(
            f"/graph/v1beta1/drives/{safe_drive}/root/invite",
            json={
                "recipients": [
                    {
                        "@libre.graph.recipient.type": "group",
                        "objectId": group_native_id,
                    }
                ],
                "roles": [role_id],
            },
        )
        resp.raise_for_status()

    # ------------------------------------------------ protected cloud mode: RO reader
    #
    # NOTE (design §9.2, §11.4): the reader here is a LibreGraph user. For real
    # login-based mount auth it must be backed by a dedicated Keycloak user, and
    # creating that on prod-private's SHARED Keycloak is an open question (§9.2).
    # The Viewer-role RO guarantee is code-read-only/unverified-live (§11.7); the
    # live status-code validation is the §11.4 manual gate before protected mode
    # may engage on OpenCloud. This slice implements the interface + the correct
    # Space Viewer invite; it does not itself provision the KC user.

    async def _find_ro_reader(self, user_key: str) -> Optional[str]:
        name = f"srw-reader-{user_key}"
        resp = await self._graph_get("/graph/v1.0/users")
        for user in resp.json().get("value", []):
            if user.get("displayName") == name:
                return str(user["id"])
        return None

    async def ensure_ro_reader(self, *, user_key: str) -> str:
        """Idempotently ensure the ``srw-reader-<user_key>`` LibreGraph user with
        no standing Space access. Returns its LibreGraph id."""
        self._ensure_ready()
        existing = await self._find_ro_reader(user_key)
        if existing:
            return existing
        name = f"srw-reader-{user_key}"
        body = {
            "accountEnabled": True,
            "displayName": name,
            # oCIS LibreGraph REQUIRES onPremisesSamAccountName on create —
            # omitting it returns 400 "no value given for required property
            # onPremisesSamAccountName" (live dev-cluster oCIS validation,
            # 2026-07-10). It doubles as the reader's login username.
            "onPremisesSamAccountName": name,
            "mail": f"{name}@srw.local",
            "identities": [{"issuer": self._keycloak_issuer, "issuerAssignedId": name}],
        }
        try:
            resp = await self._graph_post("/graph/v1.0/users", json=body)
            if resp.status_code == 409 or (
                resp.status_code == 400 and "nameAlreadyExists" in resp.text
            ):
                found = await self._find_ro_reader(user_key)
                if found:
                    return found
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        return str(resp.json()["id"])

    async def mint_ro_grant(
        self, handle: ProjectFolderHandle, *, user_key: str, grant_key: str
    ) -> RoReaderGrant:
        """Invite the reader user to the Space with the **Viewer** role
        (read-only), capturing the permission id for later revoke."""
        self._ensure_ready()
        drive_id = handle.native_id
        reader_id = await self._find_ro_reader(user_key)
        if not reader_id:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"RO reader for {user_key!r} not provisioned",
                backend=self.backend_id,
            )
        role_id = await self._role_id(
            _SPACE_VIEWER_ROLE_NAME, _SPACE_VIEWER_ROLE_WEIGHT
        )
        safe_drive = quote(drive_id, safe="")
        try:
            resp = await self._graph_post(
                f"/graph/v1beta1/drives/{safe_drive}/root/invite",
                json={
                    "recipients": [
                        {"@libre.graph.recipient.type": "user", "objectId": reader_id}
                    ],
                    "roles": [role_id],
                },
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        value = resp.json().get("value") or []
        if not value or "id" not in value[0]:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                "Space invite returned no permission id",
                backend=self.backend_id,
                raw=resp.json(),
            )
        permission_id = str(value[0]["id"])
        webdav_url = f"{self._base_url}/dav/spaces/{drive_id}/"
        grant_handle = json.dumps(
            {
                "permission_id": permission_id,
                "drive_id": str(drive_id),
                "reader_id": reader_id,
                "user_key": user_key,
            }
        )
        return RoReaderGrant(
            reader_id=reader_id,
            grant_handle=grant_handle,
            webdav_url=webdav_url,
            credentials=None,  # OC reader authenticates with a short-TTL bearer
            auth_kind="keycloak_user_impersonation",
        )

    async def revoke_ro_grant(self, grant_handle: str, *, user_key: str) -> None:
        """Delete the Space Viewer permission minted by ``mint_ro_grant``.
        Idempotent — a 404 (already gone) is not an error."""
        self._ensure_ready()
        data = json.loads(grant_handle)
        safe_drive = quote(data["drive_id"], safe="")
        permission_id = data["permission_id"]
        try:
            resp = await self._graph_delete(
                f"/graph/v1beta1/drives/{safe_drive}/root/permissions/{permission_id}"
            )
            if resp.status_code not in (200, 204, 404):
                resp.raise_for_status()
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e

    async def seed_canary_fixture(self, handle: ProjectFolderHandle) -> CanaryFixture:
        """Write a real canary file with the write identity so the RO probe's
        CVE side channels can target a real path (design §11.4)."""
        path = ".srw-ro-canary/probe.txt"
        await self.put_project_folder_file_bytes(handle, path=path, content=b"canary")
        return CanaryFixture(path=path, version_ref=None, trash_ref=None)

    async def remove_canary_fixture(
        self, handle: ProjectFolderHandle, fixture: CanaryFixture
    ) -> None:
        try:
            await self.delete_project_folder_file(
                handle, path=fixture.path, if_exists=True
            )
        except CloudBackendError:
            pass

    # ---------------------------------------------------------------- Agent home

    async def _ensure_agent_home_space(self) -> tuple[str, Optional[str]]:
        """Create or fetch the agent-home Space; return (drive_id, web_url).

        Session folders live under this Space — it is analogous to the
        ``agent-service`` user home in the Nextcloud backend. Searched by
        the fixed display name ``srw-agent-home``; created if missing.

        ``web_url`` is the authoritative browser deep-link from the drive
        response; callers should prefer it over constructing
        ``/files/spaces/{drive_id}`` themselves (OpenCloud Web routes by
        drive alias, not raw id).
        """
        # Look up an existing space first. ``/graph/v1.0/drives`` lists
        # every drive the caller can see, which for the service account
        # includes project Spaces too, so we filter by name client-side.
        resp = await self._graph_get(
            "/graph/v1.0/drives",
            params={"$filter": f"name eq '{_AGENT_HOME_SPACE_NAME}'"},
        )
        for drive in resp.json().get("value", []):
            if drive.get("name") == _AGENT_HOME_SPACE_NAME:
                web_url = (drive.get("webUrl") or "").rstrip("/") or None
                return str(drive["id"]), web_url

        # Not found — create it.
        resp = await self._graph_post(
            "/graph/v1.0/drives",
            json={"name": _AGENT_HOME_SPACE_NAME},
        )
        resp.raise_for_status()
        drive = resp.json()
        drive_id = drive.get("id")
        if not drive_id:
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                "agent-home Space creation returned no id",
                backend=self.backend_id,
                raw=drive,
            )
        web_url = (drive.get("webUrl") or "").rstrip("/") or None
        logger.info("Created OpenCloud agent-home Space drive_id=%s", drive_id)
        return str(drive_id), web_url

    # ---------------------------------------------------------------- Item lookup

    async def _resolve_item_id(self, drive_id: str, folder_path: str) -> str:
        """Look up a folder's LibreGraph item id by path, via WebDAV PROPFIND.

        Returns the compound fileid ``{drive_id}!{item_uuid}`` that the
        ``/graph/v1beta1/drives/{id}/items/{item}/invite`` endpoint
        expects.

        We use WebDAV here instead of LibreGraph's native
        ``/graph/v1.0/drives/{id}/root:/{path}`` form because the latter
        returns 404 for subfolders of project Spaces on current OpenCloud
        (tested against docker.io/opencloudeu/opencloud:latest as of
        2026-04-23). PROPFIND on the corresponding ``/dav/spaces/`` URL
        reliably returns the item's ``oc:fileid``, which is the compound
        id the /invite endpoint wants.

        Raises NOT_FOUND if the folder doesn't exist.
        """
        assert self._client is not None
        safe_drive = quote(drive_id, safe="")
        safe_path = quote(folder_path.strip("/"), safe="/")
        token = await self._get_service_token()
        try:
            resp = await self._client.request(
                "PROPFIND",
                f"/dav/spaces/{safe_drive}/{safe_path}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Depth": "0",
                },
            )
        except httpx.HTTPError as e:
            raise self._map_http_error(e) from e
        if resp.status_code == 404:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"item {folder_path!r} not found in drive {drive_id}",
                backend=self.backend_id,
                status_code=404,
            )
        if resp.status_code not in (200, 207):
            raise CloudBackendError(
                CloudBackendErrorKind.UNKNOWN,
                (
                    f"unexpected PROPFIND status {resp.status_code} "
                    f"for {folder_path!r} in drive {drive_id}"
                ),
                backend=self.backend_id,
                status_code=resp.status_code,
                raw={"body": resp.text[:500]},
            )
        match = re.search(r"<oc:fileid>([^<]+)</oc:fileid>", resp.text)
        if not match:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"oc:fileid missing in PROPFIND response for {folder_path!r}",
                backend=self.backend_id,
                raw={"body": resp.text[:500]},
            )
        return match.group(1)
