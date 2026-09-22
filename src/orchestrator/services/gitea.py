"""Gitea client for workspace delivery.

Provides a GiteaClient that bootstraps an admin user on a Gitea instance
and creates per-job repositories. Agents push workspace contents to these
repos so users can browse deliverables via Gitea's web UI.

Gracefully degrades — if Gitea is unavailable, all methods return safe
defaults and the system continues without workspace delivery.
"""

import asyncio
import logging
import os
import re
import secrets
import shlex
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import quote, unquote, urlparse
from uuid import UUID

import httpx

from shared.repo_path_safety import REPO_NAME_RE, check_repo_path_shape

logger = logging.getLogger(__name__)


class GiteaPathError(ValueError):
    """A caller-shaped repository path, git ref, or name refused at the sink.

    Raised before any request leaves the process. This client authenticates
    as the Gitea instance administrator and every managed repository lives
    under one owner, so a ``..`` segment spliced into a URL -- which httpx
    normalises away before sending -- would re-target the request at any
    other repository in the instance (security audit 2026-08-27, findings
    #3/#4: ``/api/jobs/{id}/repo/file``, ``/repo/contents`` and the MCP
    ``get_job_file`` tool all funnel into this client). The validation
    therefore lives here, once, rather than in each route.
    """


def validate_gitea_name(name: str, *, kind: str = "repository") -> str:
    """Return ``name`` unchanged if it is a usable Gitea owner/repo/user name.

    Gitea's own name charset (``validation.AlphaDashDotPattern``, the shared
    :data:`~shared.repo_path_safety.REPO_NAME_RE`), so a validated name is
    already one safe URL path segment. ``kind`` only labels the error. ``.``,
    ``..`` and any name containing ``..`` are refused outright: Gitea reserves
    the first two and nothing the orchestrator manages is ever named with the
    third.
    """
    if not isinstance(name, str) or not name:
        raise GiteaPathError(f"Gitea {kind} name must be a non-empty string")
    if not REPO_NAME_RE.fullmatch(name) or ".." in name or name == ".":
        raise GiteaPathError(f"Gitea {kind} name {name!r} is not allowed")
    return name


def _validated_repo_path(value: str, *, what: str, allow_empty: bool) -> str:
    if not isinstance(value, str):
        raise GiteaPathError(f"{what} must be a string")
    # Exactly one trailing slash is directory spelling (``docs/``), not an
    # empty segment; a second one (``docs//``) still is.
    if value.endswith("/"):
        value = value[:-1]
    if value == "":
        if allow_empty:
            return ""
        raise GiteaPathError(f"{what} must not be empty")
    # Raw and percent-decoded form alike: ``..%2F`` is one decode away from
    # the same traversal, so both must pass before the raw form is encoded.
    check_repo_path_shape(value, what=what, error=GiteaPathError)
    return value


def encode_repo_path(path: str, *, allow_empty: bool = False) -> str:
    """Validate a repository-relative path and percent-encode it per segment.

    Rejects absolute paths, ``.`` and ``..`` segments, empty segments
    (``a//b``), backslashes, NUL and other control characters, and any value
    whose percent-decoded form would fail those rules (``..%2F``). Every
    surviving segment is ``quote(segment, safe="")``-encoded and the segments
    are re-joined with ``/``: the separators are the only bytes that reach
    the URL unencoded, so nothing the caller wrote can change which
    repository the request addresses. ``allow_empty`` admits ``""`` (the
    repository root) and returns it unchanged.
    """
    raw = _validated_repo_path(path, what="Repository path", allow_empty=allow_empty)
    if not raw:
        return ""
    return "/".join(quote(segment, safe="") for segment in raw.split("/"))


def encode_compare_ref(ref: str) -> str:
    """Validate one side of a ``compare/{base}...{head}`` pair.

    Same rules as :func:`encode_repo_path` (slashes stay literal — a branch
    is spelled ``job/abc`` here, not ``job%2Fabc``), plus a refusal of ``:``.
    Gitea's compare route reads ``{owner}:{branch}`` as a cross-repository
    reference; on the REST API that resolution is confined to real forks, so
    SRW's sibling job repositories are not reachable through it today. That
    is Gitea's invariant, not ours, and it is the only thing standing between
    a caller-supplied ``base``/``head`` and another repository — so refuse the
    separator here rather than inherit an upstream guarantee we do not own.
    A legitimate ref cannot contain ``:`` (git forbids it in ref names).
    """
    raw = _validated_repo_path(ref, what="Compare ref", allow_empty=False)
    if ":" in raw or ":" in unquote(raw):
        raise GiteaPathError("Compare ref contains a repository separator")
    return "/".join(quote(segment, safe="") for segment in raw.split("/"))


def encode_repo_ref(ref: str) -> str:
    """Validate a git ref and encode it as exactly one URL path segment.

    Same shape rules as :func:`encode_repo_path` (git itself forbids ``..``,
    control characters and empty components in ref names, so no legitimate
    ref is refused). Slashes become ``%2F`` because Gitea's
    ``git/trees/{sha}`` and ``branches/*`` routes take the ref as a single
    parameter: ``job/abc`` must travel as ``job%2Fabc``.
    """
    raw = _validated_repo_path(ref, what="Git ref", allow_empty=False)
    return quote(raw, safe="")


class GiteaClient:
    """Async HTTP client for Gitea API.

    Reads configuration from environment variables:
        GITEA_INTERNAL_URL: In-cluster service URL for the orchestrator's
            own API calls (e.g. http://srw-gitea:3000). Preferred base.
        GITEA_URL: Browser-facing base URL (e.g. https://git.localhost).
            Used only as a fallback for the API base; may resolve to
            loopback and be unreachable from inside the cluster.
        GITEA_ADMIN_USER: Admin username to create/use (default: ``srw``)
        GITEA_ADMIN_PASSWORD: Admin password. No default — it must come from
            the deployment's secret. A build-time fallback would ship a
            known credential that silently becomes the real admin password
            of any install that forgets to set one.
    """

    def __init__(self) -> None:
        # Server-to-server API base: prefer the in-cluster URL so the
        # orchestrator can always reach Gitea from inside the pod. GITEA_URL
        # (browser-facing, e.g. https://git.localhost) may resolve to
        # loopback in-cluster and be unreachable here. Clone/web URLs handed
        # to users are built separately (see _build_clone_url, which also
        # prefers the internal URL).
        self._url = (
            os.environ.get("GITEA_INTERNAL_URL") or os.environ.get("GITEA_URL", "")
        ).rstrip("/")
        self._user = os.environ.get("GITEA_ADMIN_USER", "srw")
        self._password = os.environ.get("GITEA_ADMIN_PASSWORD", "")
        self._initialized = False
        self._client: Optional[httpx.AsyncClient] = None

    @property
    def is_configured(self) -> bool:
        """True if GITEA_URL is set."""
        return bool(self._url)

    @property
    def is_initialized(self) -> bool:
        """True if admin user and access are verified."""
        return self._initialized

    @property
    def repository_owner(self) -> str:
        """Server-owned Gitea namespace for managed repositories."""
        return self._user

    def clean_repo_url(self, repo_name: str) -> str:
        """Credential-free canonical URL safe for durable state."""
        return self._build_clone_url(repo_name)

    def _repo_api_url(self, repo_name: str, *segments: str) -> str:
        """Build ``/api/v1/repos/{owner}/{repo}[/segments...]``.

        The owner and repository names are validated here; every
        caller-shaped value in ``segments`` must already have gone through
        :func:`encode_repo_path` / :func:`encode_repo_ref`, so the only raw
        strings that reach the URL are literal API words. Empty segments
        (the repository root from ``encode_repo_path(..., allow_empty=True)``)
        are dropped rather than emitted as a trailing slash.
        """
        owner = validate_gitea_name(self._user, kind="owner")
        repo = validate_gitea_name(repo_name)
        url = f"{self._url}/api/v1/repos/{owner}/{repo}"
        tail = "/".join(segment for segment in segments if segment)
        return f"{url}/{tail}" if tail else url

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create the httpx client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                auth=(self._user, self._password),
            )
        return self._client

    async def ensure_initialized(self) -> bool:
        """Bootstrap admin user and verify access.

        Creates the admin user via Gitea's sign-up API (first user becomes
        admin). If the user already exists, verifies credentials work.

        Returns:
            True if Gitea is ready, False if unavailable or setup failed.
        """
        if not self.is_configured:
            logger.info(
                "Gitea not configured (GITEA_URL not set), workspace delivery disabled"
            )
            return False

        if not self._password:
            logger.warning(
                "Gitea configured but GITEA_ADMIN_PASSWORD is empty — "
                "workspace delivery disabled. Set it from the deployment secret."
            )
            return False

        client = self._get_client()

        # Check if Gitea is reachable (unauthenticated — avoids 401 when user doesn't exist yet)
        try:
            async with httpx.AsyncClient(timeout=30.0) as anon:
                resp = await anon.get(f"{self._url}/api/v1/version")
            if resp.status_code != 200:
                logger.warning(f"Gitea not reachable (status {resp.status_code})")
                return False
            logger.info(f"Gitea reachable: {resp.json().get('version', 'unknown')}")
        except httpx.HTTPError as e:
            logger.warning(f"Gitea not reachable: {e}")
            return False

        # Try to authenticate with existing user
        try:
            resp = await client.get(f"{self._url}/api/v1/user")
            if resp.status_code == 200:
                logger.info(f"Gitea admin user '{self._user}' authenticated")
                self._initialized = True
                return True
        except httpx.HTTPError:
            pass

        # User doesn't exist — create via sign-up (first user = admin)
        try:
            signup_data = {
                "username": self._user,
                "email": f"{self._user}@srw.local",
                "password": self._password,
                "must_change_password": False,
                "send_notify": False,
            }
            # Use unauthenticated request for sign-up
            async with httpx.AsyncClient(timeout=30.0) as anon_client:
                resp = await anon_client.post(
                    f"{self._url}/api/v1/admin/users",
                    json=signup_data,
                    auth=(self._user, self._password),
                )

                if resp.status_code in (201, 422):
                    # 201 = created, 422 = already exists
                    if resp.status_code == 422:
                        logger.info(
                            "Admin user creation returned 422 (may already exist)"
                        )
                    else:
                        logger.info(f"Created Gitea admin user '{self._user}'")
                else:
                    # Try user registration endpoint as fallback.
                    # Fetch the sign-up page first to get a CSRF token.
                    import re as _re

                    page_resp = await anon_client.get(f"{self._url}/user/sign_up")
                    csrf_token = ""
                    if page_resp.status_code == 200:
                        m = _re.search(
                            r'name="_csrf"\s+(?:content|value)="([^"]+)"',
                            page_resp.text,
                        )
                        if m:
                            csrf_token = m.group(1)

                    resp = await anon_client.post(
                        f"{self._url}/user/sign_up",
                        data={
                            "_csrf": csrf_token,
                            "user_name": self._user,
                            "email": f"{self._user}@srw.local",
                            "password": self._password,
                            "retype": self._password,
                        },
                        cookies=page_resp.cookies,
                    )
                    if resp.status_code in (200, 302, 303):
                        logger.info(
                            f"Registered Gitea user '{self._user}' via sign-up form"
                        )
                    else:
                        logger.warning(
                            "Failed to create Gitea user (status %s)",
                            resp.status_code,
                        )
                        return False

        except httpx.HTTPError as e:
            logger.warning(f"Failed to create Gitea admin user: {e}")
            return False

        # Verify access after creation
        try:
            resp = await client.get(f"{self._url}/api/v1/user")
            if resp.status_code == 200:
                self._initialized = True
                logger.info("Gitea workspace delivery initialized")
                return True
            else:
                logger.warning(
                    f"Gitea auth verification failed (status {resp.status_code})"
                )
                return False
        except httpx.HTTPError as e:
            logger.warning(f"Gitea auth verification failed: {e}")
            return False

    async def ensure_oidc_configured(self) -> bool:
        """Register Keycloak as OIDC auth source in Gitea if not already present.

        Reads configuration from environment variables:
            GITEA_OIDC_CLIENT_SECRET: OIDC client secret (required — skips if absent)
            KEYCLOAK_URL: Internal Keycloak URL for server-to-server calls
            KEYCLOAK_ISSUER_URL: Public/browser-facing Keycloak URL
            KEYCLOAK_REALM: Keycloak realm name (default: srw)

        The auth URL uses the public URL (browser navigates to it).
        Token/profile/discovery URLs use the internal URL (server-to-server).

        Returns:
            True if OIDC is configured (or was already), False if skipped/failed.
        """
        if not self._initialized:
            return False

        client_secret = os.environ.get("GITEA_OIDC_CLIENT_SECRET", "")
        if not client_secret:
            logger.info("GITEA_OIDC_CLIENT_SECRET not set, skipping Gitea OIDC setup")
            return False

        keycloak_internal = os.environ.get("KEYCLOAK_URL", "").rstrip("/")
        keycloak_public = os.environ.get("KEYCLOAK_ISSUER_URL", "").rstrip("/")
        realm = os.environ.get("KEYCLOAK_REALM", "srw")

        if not keycloak_internal or not keycloak_public:
            logger.warning(
                "KEYCLOAK_URL or KEYCLOAK_ISSUER_URL not set, skipping Gitea OIDC setup"
            )
            return False

        provider_name = "Keycloak"
        client = self._get_client()

        # Check if already configured via admin auth API (Gitea 1.23+)
        # Older versions (1.22) don't expose this endpoint — auth sources are
        # managed via CLI initContainer in the K8s deployment instead.
        try:
            resp = await client.get(f"{self._url}/api/v1/admin/auths")
            if resp.status_code == 404:
                logger.info(
                    "Gitea admin auth API not available (version <1.23), "
                    "OIDC setup handled by deployment initContainer"
                )
                return False
            if resp.status_code == 200:
                sources = resp.json()
                for src in sources:
                    if src.get("name") == provider_name:
                        logger.info(
                            f"Gitea OIDC auth source '{provider_name}' already configured"
                        )
                        return True
            else:
                logger.warning(
                    f"Failed to list Gitea auth sources (status {resp.status_code})"
                )
                return False
        except (httpx.HTTPError, Exception) as e:
            logger.warning(f"Failed to check Gitea auth sources: {e}")
            return False

        # Build OIDC URLs (split: public for browser, internal for server-to-server)
        base_internal = f"{keycloak_internal}/realms/{realm}/protocol/openid-connect"
        base_public = f"{keycloak_public}/realms/{realm}/protocol/openid-connect"

        payload = {
            "type": 6,
            "name": provider_name,
            "is_active": True,
            "oauth2_config": {
                "provider": "openidConnect",
                "client_id": "gitea",
                "client_secret": client_secret,
                "open_id_connect_auto_discovery_url": (
                    f"{keycloak_internal}/realms/{realm}/.well-known/openid-configuration"
                ),
                "custom_url_mapping": {
                    "auth_url": f"{base_public}/auth",
                    "token_url": f"{base_internal}/token",
                    "profile_url": f"{base_internal}/userinfo",
                },
                "group_claim_name": "groups",
                "admin_group": "admin",
                "skip_local_2fa": True,
            },
        }

        try:
            resp = await client.post(
                f"{self._url}/api/v1/admin/auths",
                json=payload,
            )
            if resp.status_code in (200, 201):
                logger.info(f"Gitea OIDC auth source '{provider_name}' created via API")
                return True

            # type=6 might be wrong for this Gitea version — retry with type=5
            if resp.status_code == 422:
                payload["type"] = 5
                resp = await client.post(
                    f"{self._url}/api/v1/admin/auths",
                    json=payload,
                )
                if resp.status_code in (200, 201):
                    logger.info(
                        f"Gitea OIDC auth source '{provider_name}' created via API (type=5)"
                    )
                    return True

            logger.warning(
                "Failed to create Gitea OIDC auth source (status %s)",
                resp.status_code,
            )
            return False
        except (httpx.HTTPError, Exception) as e:
            logger.warning(f"Failed to create Gitea OIDC auth source: {e}")
            return False

    @staticmethod
    def _repository_intent_description(intent_marker: str) -> str:
        marker = str(UUID(str(intent_marker)))
        return f"SRW managed repository; creation-intent={marker}"

    async def repository_creation_intent_status(
        self, name: str, *, intent_marker: str
    ) -> str:
        """Return ``match``, ``missing``, ``conflict`` or ``unavailable``.

        Only an exact server-generated marker in the exact configured owner
        namespace can make a committed-but-lost create response adoptable.
        """

        if not self._initialized:
            return "unavailable"
        try:
            marker_description = self._repository_intent_description(intent_marker)
        except (TypeError, ValueError):
            return "conflict"
        url = self._repo_api_url(name)
        try:
            response = await self._get_client().get(url)
        except httpx.HTTPError:
            return "unavailable"
        if response.status_code == 404:
            return "missing"
        if response.status_code != 200:
            return "unavailable"
        try:
            repository = response.json()
            owner = repository.get("owner") or {}
            owner_name = owner.get("login") or owner.get("username")
            if (
                str(repository.get("name") or "") == name
                and str(owner_name or "") == self._user
                and str(repository.get("description") or "") == marker_description
            ):
                return "match"
        except (TypeError, ValueError):
            pass
        return "conflict"

    async def create_repo(self, name: str, *, intent_marker: str) -> Optional[str]:
        """Create or exactly re-adopt one durable creation intent.

        Args:
            name: Repository name (e.g. "job-abc123")

        Returns:
            Credential-free clone/display URL or None if creation failed.
        """
        if not self._initialized:
            return None

        validate_gitea_name(name)
        client = self._get_client()

        try:
            resp = await client.post(
                f"{self._url}/api/v1/user/repos",
                json={
                    "name": name,
                    "private": True,
                    "auto_init": True,
                    "description": self._repository_intent_description(intent_marker),
                },
            )

            if resp.status_code == 409:
                status = await self.repository_creation_intent_status(
                    name, intent_marker=intent_marker
                )
                if status == "match":
                    return self._build_clone_url(name)
                logger.warning("Managed Gitea repository creation collision refused")
                return None
            elif resp.status_code not in (200, 201):
                logger.warning(
                    "Failed to create Gitea repo '%s' (status %s)",
                    name,
                    resp.status_code,
                )
                return None

            status = await self.repository_creation_intent_status(
                name, intent_marker=intent_marker
            )
            if status != "match":
                logger.warning("Managed Gitea repository intent verification failed")
                return None
            return self._build_clone_url(name)

        except (httpx.HTTPError, TypeError, ValueError):
            # The POST may have committed even though its response was lost.
            # Re-read by exact marker; never infer ownership from the name.
            status = await self.repository_creation_intent_status(
                name, intent_marker=intent_marker
            )
            if status == "match":
                return self._build_clone_url(name)
            logger.warning("Managed Gitea repository creation failed")
            return None

    async def delete_repo(self, name: str, *, intent_marker: str | None = None) -> bool:
        """Delete a repository.

        Args:
            name: Repository name

        Returns:
            True if deleted, False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()

        if intent_marker is not None:
            intent_status = await self.repository_creation_intent_status(
                name, intent_marker=intent_marker
            )
            if intent_status == "missing":
                return True
            if intent_status != "match":
                logger.warning("Managed Gitea repository cleanup marker refused")
                return False

        url = self._repo_api_url(name)
        try:
            resp = await client.delete(url)
            if resp.status_code == 204:
                logger.info(f"Deleted Gitea repo '{name}'")
                return True
            elif resp.status_code == 404:
                logger.debug(f"Gitea repo '{name}' not found (already deleted)")
                return True
            else:
                logger.warning(
                    f"Failed to delete Gitea repo '{name}' (status {resp.status_code})"
                )
                return False
        except httpx.HTTPError as e:
            logger.warning(f"Failed to delete Gitea repo '{name}': {e}")
            return False

    def _build_clone_url(self, repo_name: str) -> str:
        """Build the credential-free canonical URL stored in durable state."""
        url = (os.environ.get("GITEA_INTERNAL_URL") or self._url).rstrip("/")
        if "://" not in url:
            url = f"http://{url}"
        parsed = urlparse(url)
        # Explicitly reconstruct netloc from hostname/port: a misconfigured
        # legacy GITEA_INTERNAL_URL containing userinfo must not perpetuate it.
        host = parsed.hostname or ""
        if not host:
            raise RuntimeError("Gitea URL has no hostname")
        netloc = host + (f":{parsed.port}" if parsed.port else "")
        base_path = parsed.path.rstrip("/")
        owner = validate_gitea_name(self._user, kind="owner")
        repo = validate_gitea_name(repo_name)
        return f"{parsed.scheme or 'http'}://{netloc}{base_path}/{owner}/{repo}.git"

    def _ssh_internal_endpoint(self) -> tuple[str, int]:
        """Return the server-to-server SSH endpoint used to prove deploy keys."""
        configured_host = os.environ.get("GITEA_SSH_INTERNAL_HOST", "").strip()
        if configured_host:
            host = configured_host
        else:
            parsed = urlparse(os.environ.get("GITEA_INTERNAL_URL") or self._url)
            host = parsed.hostname or ""
        if not host:
            raise RuntimeError("Gitea SSH host is not configured")
        try:
            port = int(os.environ.get("GITEA_SSH_INTERNAL_PORT", "2222"))
        except ValueError as exc:
            raise RuntimeError("Gitea SSH port is invalid") from exc
        if not 1 <= port <= 65535:
            raise RuntimeError("Gitea SSH port is invalid")
        return host, port

    @staticmethod
    def _normalized_public_key(value: str) -> str:
        fields = str(value or "").strip().split()
        return " ".join(fields[:2]) if len(fields) >= 2 else ""

    async def ensure_repo_deploy_key(
        self,
        repo_name: str,
        *,
        title: str,
        public_key: str,
        access_mode: str,
    ) -> int | None:
        """Register one exact-mode deploy key idempotently.

        The public key is safe but still never included in log text.  A replay
        after an orchestrator crash lists the repository keys and adopts the
        exact public-key match rather than creating a second authority.
        """
        if not self._initialized:
            return None
        if access_mode not in {"read", "write"}:
            return None
        client = self._get_client()
        endpoint = self._repo_api_url(repo_name, "keys")
        wanted = self._normalized_public_key(public_key)
        wanted_read_only = access_mode == "read"

        async def _find_exact_mode_key() -> tuple[int | None, int | None]:
            """Return (matching id, opposite-mode id) for the exact key."""

            response = await client.get(endpoint)
            if response.status_code == 404:
                return None, None
            if response.status_code != 200:
                raise httpx.HTTPStatusError(
                    "deploy-key lookup failed",
                    request=response.request,
                    response=response,
                )
            for item in response.json():
                if self._normalized_public_key(item.get("key")) != wanted:
                    continue
                key_id = item.get("id")
                if key_id is None:
                    return None, None
                if bool(item.get("read_only")) is wanted_read_only:
                    return int(key_id), None
                return None, int(key_id)
            return None, None

        try:
            matching_id, opposite_id = await _find_exact_mode_key()
            if matching_id is not None:
                return matching_id
            if opposite_id is not None:
                # Mode is repository authority. Remove only the exact public
                # key before recreating it with the required least privilege.
                removed = await client.delete(f"{endpoint}/{opposite_id}")
                if removed.status_code not in (204, 404):
                    return None

            response = await client.post(
                endpoint,
                json={
                    "title": title,
                    "key": public_key,
                    "read_only": wanted_read_only,
                },
            )
            if response.status_code not in (200, 201):
                # Two orchestrator replicas can race after sharing the same
                # durable reservation. Gitea accepts one POST and may return a
                # conflict to the other. Re-read the exact public key: adopting
                # that same writable registration is idempotent; adopting a
                # merely title-matched or different key is forbidden.
                matching_id, _opposite_id = await _find_exact_mode_key()
                if matching_id is not None:
                    return matching_id
                logger.warning(
                    "Managed repository deploy-key registration failed (status %s)",
                    response.status_code,
                )
                return None
            key_id = response.json().get("id")
            return int(key_id) if key_id is not None else None
        except (httpx.HTTPError, ValueError, TypeError):
            logger.warning("Managed repository deploy-key registration failed")
            return None

    async def delete_repo_deploy_key(self, repo_name: str, key_id: int) -> bool:
        """Revoke one exact repository deploy key; 404 is idempotent success."""
        if not self._initialized:
            return False
        endpoint = self._repo_api_url(repo_name, "keys")
        try:
            response = await self._get_client().delete(f"{endpoint}/{int(key_id)}")
            return response.status_code in (204, 404)
        except (httpx.HTTPError, TypeError, ValueError):
            logger.warning("Managed repository deploy-key revocation failed")
            return False

    async def probe_repo_deploy_key(
        self,
        repo_name: str,
        *,
        private_key: str,
        access_mode: str,
        target_repo_name: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> bool:
        """Prove Git read authority through Gitea SSH without logging secrets.

        ``target_repo_name`` exists for the negative isolation gate: the key
        minted for ``repo_name`` must fail against a different private repo.
        The private key is written only to a 0600 temporary file and removed by
        the temporary-directory context even across timeout/cancellation.
        """
        if access_mode not in {"read", "write"}:
            return False
        try:
            host, port = self._ssh_internal_endpoint()
        except RuntimeError:
            return False
        owner = validate_gitea_name(self._user, kind="owner")
        target = validate_gitea_name(target_repo_name or repo_name)
        remote = f"ssh://{host}:{port}/{owner}/{target}.git"
        with tempfile.TemporaryDirectory(prefix="srw-repo-authority-") as temp_dir:
            key_path = Path(temp_dir) / "identity"
            key_path.write_text(private_key, encoding="utf-8")
            key_path.chmod(0o600)
            ssh_command = " ".join(
                shlex.quote(part)
                for part in (
                    "ssh",
                    "-i",
                    str(key_path),
                    "-l",
                    "git",
                    "-p",
                    str(port),
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "StrictHostKeyChecking=accept-new",
                    "-o",
                    f"UserKnownHostsFile={Path(temp_dir) / 'known_hosts'}",
                )
            )
            # Do not copy the orchestrator environment into a Git/SSH child:
            # it contains the Gitea administrator password and unrelated
            # service credentials. The probe needs only an executable path,
            # an isolated HOME for SSH state, and its exact SSH command.
            env = {
                "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
                "HOME": temp_dir,
                "LANG": os.environ.get("LANG", "C.UTF-8"),
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_SSH_COMMAND": ssh_command,
            }
            try:
                process = await asyncio.create_subprocess_exec(
                    "git",
                    "ls-remote",
                    remote,
                    "HEAD",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=env,
                )
                await asyncio.wait_for(
                    process.communicate(), timeout=max(1.0, timeout_seconds)
                )
            except (OSError, asyncio.TimeoutError):
                if "process" in locals() and process.returncode is None:
                    process.kill()
                    await process.wait()
                return False
            return process.returncode == 0

    @staticmethod
    def mask_credentials(url: str) -> str:
        """Mask credentials in a URL for safe logging.

        Args:
            url: URL potentially containing user:pass@

        Returns:
            URL with password replaced by ***
        """
        return re.sub(r"://([^:]+):[^@]+@", r"://\1:***@", url)

    async def get_file(
        self, repo_name: str, file_path: str, ref: str | None = None
    ) -> dict | None:
        """Read a file from a repository via Gitea API.

        Args:
            repo_name: Repository name (e.g. "job-abc123")
            file_path: Path within the repo (e.g. "output/job_frozen.json")
            ref: Branch/tag/commit (defaults to repo default branch)

        Returns:
            Decoded file content dict (parsed JSON) or None if not found/failed.
        """
        if not self._initialized:
            return None

        import base64

        client = self._get_client()
        params = {"ref": ref} if ref else {}
        url = self._repo_api_url(repo_name, "contents", encode_repo_path(file_path))

        try:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to read {file_path} from {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            data = resp.json()
            content_b64 = data.get("content", "")
            decoded = base64.b64decode(content_b64).decode("utf-8")

            import json

            return json.loads(decoded)

        except Exception as e:
            logger.warning(f"Failed to read {file_path} from {repo_name}: {e}")
            return None

    async def create_or_update_file(
        self,
        repo_name: str,
        file_path: str,
        content: str,
        message: str,
        branch: str | None = None,
    ) -> bool:
        """Create or update a file in a repository via Gitea API.

        Args:
            repo_name: Repository name (e.g. "job-abc123")
            file_path: Path within the repo (e.g. "output/job_completion.json")
            content: File content as string
            message: Commit message
            branch: Target branch (defaults to repo's default branch)

        Returns:
            True if successful, False otherwise.
        """
        if not self._initialized:
            return False

        import base64

        client = self._get_client()
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")

        contents_url = self._repo_api_url(
            repo_name, "contents", encode_repo_path(file_path)
        )

        try:
            # Check if file already exists (need SHA for update)
            params = {"ref": branch} if branch else {}
            resp = await client.get(contents_url, params=params)

            payload: dict = {
                "content": content_b64,
                "message": message,
            }

            if branch:
                payload["branch"] = branch

            if resp.status_code == 200:
                # File exists — include SHA and PUT to update.
                existing = resp.json()
                payload["sha"] = existing["sha"]
                resp = await client.put(contents_url, json=payload)
            else:
                # File doesn't exist — POST to create. Gitea returns 422
                # ``[SHA]: Required`` on PUT for non-existent files in some
                # versions, so we must split the path.
                resp = await client.post(contents_url, json=payload)

            if resp.status_code in (200, 201):
                return True

            logger.warning(
                f"Failed to write {file_path} to {repo_name} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return False

        except Exception as e:
            logger.warning(f"Failed to write {file_path} to {repo_name}: {e}")
            return False

    async def change_files(
        self,
        repo_name: str,
        branch: str,
        files: list[dict],
        message: str,
    ) -> bool:
        """Write multiple files in a SINGLE commit via Gitea's ChangeFiles API.

        Args:
            repo_name: Repository name.
            branch: Target branch.
            files: list of ``{"path": str, "content_b64": str}`` with an
                optional ``"operation"`` of ``create`` (default) or ``update``
                (base64 content keeps binary files byte-faithful). ``update``
                needs no blob SHA: Gitea validates against the branch head
                commit when none is given — but ``create`` on an existing path
                is a 422, so callers writing over unknown state must pick the
                operation per file (see the curated merge in
                ``services.project_loops``).
            message: Commit message.

        Returns:
            True on success, False otherwise.
        """
        if not self._initialized:
            return False
        if not files:
            return True

        client = self._get_client()
        payload = {
            "branch": branch,
            "message": message,
            "files": [
                {
                    "operation": f.get("operation", "create"),
                    "path": f["path"],
                    "content": f["content_b64"],
                    # Compare-and-swap (kb_gardening G3): when the caller
                    # names the blob it read, Gitea refuses the update with
                    # 422 "sha does not match" if the file moved meanwhile.
                    # Omitted (the historical default) Gitea validates only
                    # against the branch head and the last writer wins.
                    **({"sha": f["sha"]} if f.get("sha") else {}),
                }
                for f in files
            ],
        }
        url = self._repo_api_url(repo_name, "contents")
        try:
            resp = await client.post(url, json=payload)
            if resp.status_code in (200, 201):
                return True
            logger.warning(
                f"change_files failed for {repo_name}@{branch} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return False
        except Exception as e:
            logger.warning(f"change_files failed for {repo_name}@{branch}: {e}")
            return False

    async def delete_path(
        self,
        repo_name: str,
        branch: str,
        path: str,
        message: str,
        expected_sha: str | None = None,
    ) -> str:
        """Remove one file with compare-and-swap semantics.

        The KB purge primitive's forge call (``services.kb_materialize``).
        ``expected_sha`` is the blob SHA the caller believes is on ``branch``;
        Gitea refuses the delete when the file's current SHA differs, which
        is exactly the lost-update guard we want. With no SHA the current
        one is looked up first (a human-authorised purge).

        Returns a verdict string rather than a bool so the caller can tell a
        race from an outage:

        * ``deleted`` — commit landed.
        * ``absent`` — no such path on the branch (already gone: success).
        * ``conflict`` — the forge refused the SHA (409/422): re-read first.
        * ``error`` — transport/auth/other failure: retryable.
        """
        if not self._initialized:
            return "error"
        client = self._get_client()
        url = self._repo_api_url(repo_name, "contents", encode_repo_path(path))
        try:
            sha = str(expected_sha or "").strip()
            if not sha:
                resp = await client.get(url, params={"ref": branch})
                if resp.status_code == 404:
                    return "absent"
                if resp.status_code != 200:
                    logger.warning(
                        f"delete_path lookup failed for {repo_name}@{branch}:{path} "
                        f"(status {resp.status_code})"
                    )
                    return "error"
                sha = str(resp.json().get("sha") or "")
                if not sha:
                    return "error"
            resp = await client.request(
                "DELETE",
                url,
                json={"sha": sha, "message": message, "branch": branch},
            )
            if resp.status_code == 200:
                return "deleted"
            if resp.status_code == 404:
                return "absent"
            if resp.status_code in (409, 422):
                logger.info(
                    f"delete_path refused for {repo_name}@{branch}:{path} "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return "conflict"
            logger.warning(
                f"delete_path failed for {repo_name}@{branch}:{path} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return "error"
        except Exception as e:
            logger.warning(f"delete_path failed for {repo_name}@{branch}:{path}: {e}")
            return "error"

    async def delete_file(
        self,
        repo_name: str,
        file_path: str,
        message: str,
        branch: str | None = None,
    ) -> bool:
        """Delete a file from a repository via Gitea API.

        Args:
            repo_name: Repository name
            file_path: Path within the repo
            message: Commit message
            branch: Target branch (defaults to repo's default branch)

        Returns:
            True if deleted, False otherwise.
        """
        if not self._initialized:
            return False

        client = self._get_client()

        url = self._repo_api_url(repo_name, "contents", encode_repo_path(file_path))

        try:
            # Get current SHA (required for delete)
            params = {"ref": branch} if branch else {}
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return True  # Already gone
            if resp.status_code != 200:
                return False

            sha = resp.json()["sha"]

            delete_payload: dict = {"sha": sha, "message": message}
            if branch:
                delete_payload["branch"] = branch

            resp = await client.request("DELETE", url, json=delete_payload)

            return resp.status_code == 200

        except Exception as e:
            logger.warning(f"Failed to delete {file_path} from {repo_name}: {e}")
            return False

    async def list_contents(
        self, repo_name: str, path: str = "", ref: str | None = None
    ) -> list[dict] | None:
        """List directory contents from a repository.

        Args:
            repo_name: Repository name
            path: Directory path within the repo (empty string for root)
            ref: Branch/tag/commit

        Returns:
            List of file/dir entries with name, path, type, size, or None on failure.
            Each entry has: name, path, type ("file"|"dir"|"submodule"), size.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        params = {"ref": ref} if ref else {}
        url_path = self._repo_api_url(
            repo_name, "contents", encode_repo_path(path, allow_empty=True)
        )

        try:
            resp = await client.get(url_path, params=params)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to list {path or '/'} in {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            data = resp.json()

            # Gitea returns a list for directories, a single object for files
            if isinstance(data, dict):
                # Single file — wrap in list for consistency
                return [data]

            return [
                {
                    "name": entry["name"],
                    "path": entry["path"],
                    "type": entry["type"],
                    "size": entry.get("size", 0),
                }
                for entry in data
            ]

        except Exception as e:
            logger.warning(f"Failed to list {path or '/'} in {repo_name}: {e}")
            return None

    async def get_file_content(
        self, repo_name: str, file_path: str, ref: str | None = None
    ) -> str | None:
        """Read raw file content as a string from a repository.

        Unlike get_file() which parses JSON, this returns the raw text content.

        Args:
            repo_name: Repository name
            file_path: Path within the repo
            ref: Branch/tag/commit

        Returns:
            File content as string, or None if not found/failed.
        """
        if not self._initialized:
            return None

        import base64

        client = self._get_client()
        params = {"ref": ref} if ref else {}
        url = self._repo_api_url(repo_name, "contents", encode_repo_path(file_path))

        try:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to read {file_path} from {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            data = resp.json()
            content_b64 = data.get("content", "")
            return base64.b64decode(content_b64).decode("utf-8")

        except Exception as e:
            logger.warning(f"Failed to read {file_path} from {repo_name}: {e}")
            return None

    async def get_file_bytes(
        self,
        repo_name: str,
        file_path: str,
        ref: str | None = None,
        *,
        redact_coordinates: bool = False,
    ) -> bytes | None:
        """Read raw file content as bytes from a repository.

        Use when the caller needs the file's original bytes (e.g. for an
        image, PDF, or anything that ``get_file_content``'s UTF-8 decode
        would corrupt). The Gitea contents API returns the file body
        base64-encoded inside JSON; we decode that and return the result.

        Returns:
            File bytes, or ``None`` if not found / failed.
        """
        if not self._initialized:
            return None

        import base64

        client = self._get_client()
        params = {"ref": ref} if ref else {}
        url = self._repo_api_url(repo_name, "contents", encode_repo_path(file_path))

        try:
            resp = await client.get(url, params=params)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                if redact_coordinates:
                    logger.warning(
                        "Failed to read private repository object bytes (status %s)",
                        resp.status_code,
                    )
                else:
                    logger.warning(
                        f"Failed to read {file_path} bytes from {repo_name} "
                        f"(status {resp.status_code})"
                    )
                return None
            data = resp.json()
            content_b64 = data.get("content", "")
            if not content_b64:
                return b""
            return base64.b64decode(content_b64)
        except Exception as e:
            if redact_coordinates:
                logger.warning("Failed to read private repository object bytes")
            else:
                logger.warning(
                    f"Failed to read {file_path} bytes from {repo_name}: {e}"
                )
            return None

    async def get_commits(
        self,
        repo_name: str,
        sha: str = "main",
        page: int = 1,
        limit: int = 20,
    ) -> list[dict] | None:
        """List commits from a branch, tag, or SHA.

        Args:
            repo_name: Repository name
            sha: Branch, tag, or commit SHA to list from
            page: Page number (1-indexed)
            limit: Max commits per page

        Returns:
            List of commit dicts with sha, message, author, date, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "commits")

        try:
            # NOTE: this must be `/commits`, not `/git/commits` — the latter
            # only resolves specific commit SHAs and 404s on branch names.
            resp = await client.get(
                url,
                params={
                    "sha": sha,
                    "page": page,
                    "limit": limit,
                    # Skip per-commit diff stats / signature checks — callers
                    # only read sha, message, author, date.
                    "stat": "false",
                    "verification": "false",
                    "files": "false",
                },
            )
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to list commits for {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            commits = resp.json()
            return [
                {
                    "sha": c["sha"],
                    "message": c.get("commit", {}).get("message", ""),
                    "author": c.get("commit", {}).get("author", {}).get("name", ""),
                    "date": c.get("commit", {}).get("author", {}).get("date", ""),
                }
                for c in commits
            ]

        except Exception as e:
            logger.warning(f"Failed to list commits for {repo_name}: {e}")
            return None

    async def get_compare(
        self, repo_name: str, base: str, head: str = "HEAD"
    ) -> dict | None:
        """Compare two refs and return commits between them.

        Args:
            repo_name: Repository name
            base: Base ref (commit SHA, tag, or branch)
            head: Head ref (default: HEAD)

        Returns:
            Dict with total_commits and commits list, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(
            repo_name,
            "compare",
            f"{encode_compare_ref(base)}...{encode_compare_ref(head)}",
        )

        try:
            resp = await client.get(
                url,
                # Same fast flags as get_commits — we only read sha/message/
                # author/date below. Gitea 1.22 only honors `files`; `stat`
                # (the expensive per-commit `git diff` subprocess) is
                # hardcoded on there and honored from 1.23. Harmless where
                # ignored, saves ~150ms/commit where not.
                params={"stat": "false", "verification": "false", "files": "false"},
            )
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to compare {base}...{head} in {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            data = resp.json()
            commits = [
                {
                    "sha": c["sha"],
                    "message": c.get("commit", {}).get("message", ""),
                    "author": c.get("commit", {}).get("author", {}).get("name", ""),
                    "date": c.get("commit", {}).get("author", {}).get("date", ""),
                }
                for c in data.get("commits", [])
            ]
            return {
                "total_commits": data.get("total_commits", len(commits)),
                "commits": commits,
            }

        except Exception as e:
            logger.warning(f"Failed to compare {base}...{head} in {repo_name}: {e}")
            return None

    async def get_commits_between(
        self,
        repo_name: str,
        since_ref: str,
        head: str = "main",
        *,
        max_commits: int = 500,
    ) -> dict | None:
        """List commits on ``head`` newer than ``since_ref`` (exclusive).

        Replaces the compare API for the "commits since job start" path.
        Two reasons: Gitea 1.22's compare endpoint 404s on SHA bases
        (``BaseNotExist``), and it computes per-commit diff stats server-side
        (one ``git diff`` subprocess per commit, ~150ms each) regardless of
        the ``stat`` query flag. The commits endpoint accepts SHA starting
        points and honors ``stat=false`` (~155x cheaper), so we page it from
        ``head`` and cut client-side at ``since_ref``.

        Returns the get_compare shape: {"total_commits", "commits"}. When
        ``since_ref`` isn't found within ``max_commits`` (not an ancestor,
        or force-pushed away), the collected commits are still returned with
        ``"truncated": True``. None when ``head`` can't be listed at all.
        """
        if not self._initialized:
            return None

        since_sha = since_ref.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{7,40}", since_sha):
            # Branch/tag base — resolve to a SHA so the cut below can match.
            resolved = await self.get_branch_head_sha(repo_name, since_ref)
            if resolved:
                since_sha = resolved.lower()
            else:
                logger.debug(
                    f"get_commits_between: could not resolve base ref "
                    f"'{since_ref}' in {repo_name}; walk will run to the cap"
                )

        page_limit = 50
        collected: list[dict] = []
        for page in range(1, max(1, max_commits // page_limit) + 1):
            batch = await self.get_commits(
                repo_name, sha=head, page=page, limit=page_limit
            )
            if batch is None:
                if page == 1:
                    return None
                logger.warning(
                    f"get_commits_between: page {page} failed for {repo_name}; "
                    f"returning partial result"
                )
                break
            for commit in batch:
                sha = commit.get("sha", "").lower()
                if sha.startswith(since_sha):
                    return {"total_commits": len(collected), "commits": collected}
                collected.append(commit)
            if len(batch) < page_limit:
                # Walked the whole history without meeting since_ref.
                break
        return {
            "total_commits": len(collected),
            "commits": collected,
            "truncated": True,
        }

    async def download_repo_archive(
        self, repo_name: str, ref: str, dest_path: str
    ) -> bool:
        """Stream a repository archive (``<ref>.tar.gz``) to ``dest_path``.

        One request for the whole tree — the bulk-read alternative to N
        per-file ``contents/`` calls (the measured kb_reindex hot path:
        ~11k sequential reads/day at ~130ms each). Accepts branch, tag, or
        commit SHA refs; entries are prefixed with one top-level directory
        (the repo name).

        Returns True on success. On any failure the file at ``dest_path``
        must be considered garbage.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(
            repo_name, "archive", f"{encode_repo_path(ref)}.tar.gz"
        )
        try:
            # Generous read timeout: Gitea generates the archive server-side
            # before the first byte (seconds for large repos, then cached).
            async with client.stream(
                "GET", url, timeout=httpx.Timeout(30.0, read=120.0)
            ) as resp:
                if resp.status_code != 200:
                    logger.warning(
                        f"Archive download for {repo_name}@{ref} failed "
                        f"(status {resp.status_code})"
                    )
                    return False
                with open(dest_path, "wb") as fh:
                    async for chunk in resp.aiter_bytes():
                        fh.write(chunk)
            return True
        except Exception as e:
            logger.warning(f"Archive download for {repo_name}@{ref} failed: {e}")
            return False

    async def get_diff(
        self, repo_name: str, base: str, head: str = "HEAD"
    ) -> str | None:
        """Get raw unified diff between two refs.

        Args:
            repo_name: Repository name
            base: Base ref (commit SHA, tag, or branch)
            head: Head ref (default: HEAD)

        Returns:
            Unified diff as text, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(
            repo_name,
            "compare",
            f"{encode_compare_ref(base)}...{encode_compare_ref(head)}.diff",
        )

        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to get diff {base}...{head} in {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            return resp.text

        except Exception as e:
            logger.warning(f"Failed to get diff {base}...{head} in {repo_name}: {e}")
            return None

    async def get_tags(
        self, repo_name: str, page: int = 1, limit: int = 50
    ) -> list[dict] | None:
        """List tags in a repository.

        Args:
            repo_name: Repository name
            page: Page number (1-indexed)
            limit: Max tags per page

        Returns:
            List of tag dicts with name, sha, and date, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "tags")

        try:
            resp = await client.get(
                url,
                params={"page": page, "limit": limit},
            )
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to list tags for {repo_name} (status {resp.status_code})"
                )
                return None

            tags = resp.json()
            return [
                {
                    "name": t["name"],
                    "sha": t.get("id", t.get("commit", {}).get("sha", "")),
                    "message": t.get("message", ""),
                }
                for t in tags
            ]

        except Exception as e:
            logger.warning(f"Failed to list tags for {repo_name}: {e}")
            return None

    # =========================================================================
    # Branch & PR Operations (Projects support)
    # =========================================================================

    async def create_branch(
        self, repo_name: str, new_branch: str, from_branch: str = "main"
    ) -> bool:
        """Create a new branch in a repository.

        Args:
            repo_name: Repository name
            new_branch: Name for the new branch
            from_branch: Branch to create from (default: main)

        Returns:
            True if created (or already exists), False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(repo_name, "branches")

        try:
            resp = await client.post(
                url,
                json={
                    "new_branch_name": new_branch,
                    "old_branch_name": from_branch,
                },
            )

            if resp.status_code in (201, 200):
                logger.info(f"Created branch '{new_branch}' in {repo_name}")
                return True
            elif resp.status_code == 409:
                logger.debug(f"Branch '{new_branch}' already exists in {repo_name}")
                return True
            else:
                logger.warning(
                    f"Failed to create branch '{new_branch}' in {repo_name} "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(
                f"Failed to create branch '{new_branch}' in {repo_name}: {e}"
            )
            return False

    async def get_branch_head_sha(
        self,
        repo_name: str,
        branch: str,
        *,
        redact_coordinates: bool = False,
    ) -> str | None:
        """Return the HEAD commit SHA for a branch.

        Uses ``GET /repos/{owner}/{repo}/branches/{branch}``. ``branch``
        may contain slashes (e.g. ``job/abc123``) — they're URL-encoded.

        Returns:
            Commit SHA on the branch's HEAD, or ``None`` if the branch
            doesn't exist or the request fails.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "branches", encode_repo_ref(branch))
        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                if redact_coordinates:
                    logger.warning(
                        "Failed to resolve private repository revision (status %s)",
                        resp.status_code,
                    )
                else:
                    logger.warning(
                        f"Failed to read branch '{branch}' on {repo_name} "
                        f"(status {resp.status_code})"
                    )
                return None
            data = resp.json()
            return data.get("commit", {}).get("id")
        except Exception as e:
            if redact_coordinates:
                logger.warning("Failed to resolve private repository revision")
            else:
                logger.warning(f"Failed to read branch '{branch}' on {repo_name}: {e}")
            return None

    async def list_tree(self, repo_name: str, ref: str) -> list[dict[str, str]] | None:
        """Recursive tree listing at a specific ref.

        Returns one entry per blob/tree under the ref's root, each as
        ``{path, type, sha}``. Used by Mode A diff capture as a
        replacement for Gitea 1.22's ``compare/{base}...{head}.diff``
        which returns 404 ``BaseNotExist`` for raw SHAs (only branches
        and tags work there, per gitea#19797 et al.). Tree comparison
        between baseline + head gives us the same ``added`` /
        ``modified`` / ``deleted`` triage without depending on the
        broken compare endpoint.

        Args:
            repo_name: Repository name
            ref: Commit SHA, branch, or tag

        Returns:
            List of ``{path, type, sha}`` for every descendant, or
            ``None`` on failure. Trees and blobs are both included;
            callers typically filter ``type == 'blob'``.
        """
        if not self._initialized:
            return None
        client = self._get_client()
        url = self._repo_api_url(repo_name, "git", "trees", encode_repo_ref(ref))
        out: list[dict[str, str]] = []
        page = 1
        per_page = 1000
        try:
            while True:
                resp = await client.get(
                    url,
                    params={"recursive": "true", "per_page": per_page, "page": page},
                )
                if resp.status_code == 404:
                    return None
                if resp.status_code != 200:
                    logger.warning(
                        f"Failed to list tree {ref} on {repo_name} "
                        f"(status {resp.status_code})"
                    )
                    return None
                data = resp.json()
                entries = data.get("tree") or []
                out.extend(entries)
                # Truncated trees: keep paging.
                truncated = data.get("truncated")
                if not truncated or not entries:
                    break
                page += 1
            return out
        except Exception as e:
            logger.warning(f"Failed to list tree {ref} on {repo_name}: {e}")
            return None

    async def list_branches(self, repo_name: str) -> list[dict] | None:
        """List all branches in a repository.

        Args:
            repo_name: Repository name

        Returns:
            List of branch dicts with name and commit info, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "branches")

        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.warning(
                    f"Failed to list branches for {repo_name} "
                    f"(status {resp.status_code})"
                )
                return None

            return [
                {
                    "name": b["name"],
                    "sha": b.get("commit", {}).get("id", ""),
                    "protected": b.get("protected", False),
                }
                for b in resp.json()
            ]

        except httpx.HTTPError as e:
            logger.warning(f"Failed to list branches for {repo_name}: {e}")
            return None

    async def delete_branch(self, repo_name: str, branch: str) -> bool:
        """Delete a branch from a repository.

        Args:
            repo_name: Repository name
            branch: Branch name to delete

        Returns:
            True if deleted (or not found), False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(repo_name, "branches", encode_repo_ref(branch))

        try:
            resp = await client.delete(url)
            if resp.status_code == 204:
                logger.info(f"Deleted branch '{branch}' from {repo_name}")
                return True
            elif resp.status_code == 404:
                logger.debug(f"Branch '{branch}' not found in {repo_name}")
                return True
            else:
                logger.warning(
                    f"Failed to delete branch '{branch}' from {repo_name} "
                    f"(status {resp.status_code})"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(f"Failed to delete branch '{branch}' from {repo_name}: {e}")
            return False

    async def create_pr(
        self,
        repo_name: str,
        title: str,
        head: str,
        base: str = "main",
        body: str = "",
    ) -> dict | None:
        """Create a pull request in a repository.

        Args:
            repo_name: Repository name
            title: PR title
            head: Head branch name
            base: Base branch name (default: main)
            body: PR description

        Returns:
            PR dict with number and url fields, or None on failure.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "pulls")

        try:
            resp = await client.post(
                url,
                json={
                    "title": title,
                    "head": head,
                    "base": base,
                    "body": body,
                },
            )

            if resp.status_code in (200, 201):
                data = resp.json()
                logger.info(f"Created PR #{data.get('number')} in {repo_name}: {title}")
                return {
                    "number": data["number"],
                    "url": data.get("html_url", ""),
                    "state": data.get("state", "open"),
                }
            else:
                logger.warning(
                    f"Failed to create PR in {repo_name} "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return None

        except httpx.HTTPError as e:
            logger.warning(f"Failed to create PR in {repo_name}: {e}")
            return None

    async def list_pull_requests(
        self,
        repo_name: str,
        *,
        state: str = "all",
        page: int = 1,
        limit: int = 50,
    ) -> list[dict] | None:
        """List pull requests with the identity fields reconciliation needs.

        ``state='all'`` is load-bearing for completion replay: the process may
        die after a PR was merged or closed, so open-only listing would turn an
        existing command-keyed PR into a false absence.  ``None`` means the
        probe was ambiguous; callers must not interpret it as an empty page.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "pulls")
        try:
            resp = await client.get(
                url,
                params={"state": state, "page": page, "limit": limit},
            )
            if resp.status_code != 200:
                logger.warning(
                    "Failed to list PRs in %s (status %s)",
                    repo_name,
                    resp.status_code,
                )
                return None
            pulls: list[dict] = []
            for raw in resp.json():
                head = raw.get("head") if isinstance(raw, dict) else None
                base = raw.get("base") if isinstance(raw, dict) else None
                pulls.append(
                    {
                        "number": raw.get("number"),
                        "title": raw.get("title") or "",
                        "body": raw.get("body") or "",
                        "state": raw.get("state") or "",
                        "head": head.get("ref") if isinstance(head, dict) else None,
                        "base": base.get("ref") if isinstance(base, dict) else None,
                    }
                )
            return pulls
        except Exception as exc:  # noqa: BLE001 - ambiguity must be explicit
            logger.warning("Failed to list PRs in %s: %s", repo_name, exc)
            return None

    async def merge_pr(
        self,
        repo_name: str,
        pr_index: int,
        merge_strategy: str = "merge",
        delete_branch_after_merge: bool = False,
    ) -> bool:
        """Merge a pull request.

        Args:
            repo_name: Repository name
            pr_index: PR number/index
            merge_strategy: Merge method — "merge", "rebase", or "squash"
            delete_branch_after_merge: Delete the head branch after merge

        Returns:
            True if merged, False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(repo_name, "pulls", str(int(pr_index)), "merge")

        try:
            resp = await client.post(
                url,
                json={
                    "Do": merge_strategy,
                    "delete_branch_after_merge": delete_branch_after_merge,
                },
            )

            if resp.status_code in (200, 204):
                logger.info(
                    f"Merged PR #{pr_index} in {repo_name} (strategy: {merge_strategy})"
                )
                return True
            else:
                logger.warning(
                    f"Failed to merge PR #{pr_index} in {repo_name} "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(f"Failed to merge PR #{pr_index} in {repo_name}: {e}")
            return False

    async def probe_pr_merged(self, repo_name: str, pr_index: int) -> bool | None:
        """Return Gitea's exact merge state for one pull request.

        Gitea's ``GET .../pulls/{index}/merge`` handler is unusual: the
        response body can disagree with the HTTP status after the handler has
        already written a 204.  The status code is therefore the complete
        protocol here -- 204 means merged, 404 means not merged, and every
        other response is ambiguous and must be retried rather than guessed.

        ``None`` also covers an uninitialized client and transport failures.
        In particular, 405 is *not* treated as already merged because Gitea
        uses it for several unrelated refusal modes.
        """
        if not self._initialized:
            return None

        client = self._get_client()
        url = self._repo_api_url(repo_name, "pulls", str(int(pr_index)), "merge")
        try:
            resp = await client.get(url)
        except httpx.HTTPError as exc:
            logger.warning(
                "Failed to probe merge state for PR #%s in %s: %s",
                pr_index,
                repo_name,
                exc,
            )
            return None

        if resp.status_code == 204:
            return True
        if resp.status_code == 404:
            return False
        logger.warning(
            "Ambiguous merge-state response for PR #%s in %s (status %s)",
            pr_index,
            repo_name,
            resp.status_code,
        )
        return None

    async def close_pr(self, repo_name: str, pr_index: int) -> bool:
        """Close a pull request WITHOUT merging it.

        Used by the curated merge (workspace_and_change_records.md §6.4): the
        contracted deliverables land on ``main`` as their own commit, and the
        PR stays behind — closed, unmerged — as the branch's audit trail.

        Args:
            repo_name: Repository name
            pr_index: PR number/index

        Returns:
            True if closed, False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(repo_name, "pulls", str(int(pr_index)))

        try:
            resp = await client.patch(url, json={"state": "closed"})
            if resp.status_code in (200, 201):
                logger.info(f"Closed PR #{pr_index} in {repo_name} (unmerged)")
                return True
            logger.warning(
                f"Failed to close PR #{pr_index} in {repo_name} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return False
        except httpx.HTTPError as e:
            logger.warning(f"Failed to close PR #{pr_index} in {repo_name}: {e}")
            return False

    async def comment_on_pr(self, repo_name: str, pr_index: int, body: str) -> bool:
        """Post a comment on a pull request.

        Gitea serves PR comments through the issues API (a PR is an issue
        with code attached), hence the ``/issues/`` path.

        Args:
            repo_name: Repository name
            pr_index: PR number/index
            body: Comment body (markdown)

        Returns:
            True if the comment was created, False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(repo_name, "issues", str(int(pr_index)), "comments")

        try:
            resp = await client.post(url, json={"body": body})
            if resp.status_code in (200, 201):
                return True
            logger.warning(
                f"Failed to comment on PR #{pr_index} in {repo_name} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return False
        except httpx.HTTPError as e:
            logger.warning(f"Failed to comment on PR #{pr_index} in {repo_name}: {e}")
            return False

    async def rename_repo(self, old_name: str, new_name: str) -> bool:
        """Rename a repository.

        Args:
            old_name: Current repository name
            new_name: New repository name

        Returns:
            True if renamed, False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(old_name)
        validate_gitea_name(new_name)

        try:
            resp = await client.patch(url, json={"name": new_name})

            if resp.status_code == 200:
                logger.info(f"Renamed Gitea repo '{old_name}' -> '{new_name}'")
                return True
            elif resp.status_code == 404:
                logger.warning(f"Gitea repo '{old_name}' not found for rename")
                return False
            else:
                logger.warning(
                    f"Failed to rename Gitea repo '{old_name}' "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(f"Failed to rename Gitea repo '{old_name}': {e}")
            return False

    # =========================================================================
    # Collaborator / Access Control
    # =========================================================================

    async def find_user_by_email(self, email: str) -> Optional[str]:
        """Find a Gitea username by email address.

        Searches Gitea's user list and filters by exact email match.
        Returns None if the user hasn't logged into Gitea yet (e.g. no
        OIDC auto-registration has occurred).

        Args:
            email: Email address to search for.

        Returns:
            Gitea username (login) if found, None otherwise.
        """
        if not self._initialized or not email:
            return None

        client = self._get_client()

        try:
            # Admin API lists all users with full email — the public /users/search
            # endpoint only matches on username/full-name, not email.
            page = 1
            while True:
                resp = await client.get(
                    f"{self._url}/api/v1/admin/users",
                    params={"page": page, "limit": 50},
                )
                if resp.status_code != 200:
                    logger.debug(
                        f"Gitea admin user list failed (status {resp.status_code})"
                    )
                    return None

                users = resp.json()
                if not users:
                    break

                for user in users:
                    if user.get("email", "").lower() == email.lower():
                        return user["login"]

                if len(users) < 50:
                    break
                page += 1

            return None

        except httpx.HTTPError as e:
            logger.warning(f"Failed to search Gitea user by email: {e}")
            return None

    async def add_collaborator(
        self, repo_name: str, username: str, permission: str = "read"
    ) -> bool:
        """Add a user as a collaborator on a repository.

        Args:
            repo_name: Repository name (owned by the service account).
            username: Gitea username to add.
            permission: Access level — "read", "write", or "admin".

        Returns:
            True if added (or already a collaborator), False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(
            repo_name, "collaborators", validate_gitea_name(username, kind="user")
        )

        try:
            resp = await client.put(url, json={"permission": permission})

            if resp.status_code in (204, 200):
                logger.debug(
                    f"Added '{username}' as {permission} collaborator on '{repo_name}'"
                )
                return True
            elif resp.status_code == 422:
                # Already a collaborator
                return True
            else:
                logger.warning(
                    f"Failed to add collaborator '{username}' on '{repo_name}' "
                    f"(status {resp.status_code}): {resp.text[:200]}"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(
                f"Failed to add collaborator '{username}' on '{repo_name}': {e}"
            )
            return False

    async def remove_collaborator(self, repo_name: str, username: str) -> bool:
        """Remove a collaborator from a repository.

        Args:
            repo_name: Repository name (owned by the service account).
            username: Gitea username to remove.

        Returns:
            True if removed (or was not a collaborator), False on failure.
        """
        if not self._initialized:
            return False

        client = self._get_client()
        url = self._repo_api_url(
            repo_name, "collaborators", validate_gitea_name(username, kind="user")
        )

        try:
            resp = await client.delete(url)

            if resp.status_code in (204, 404):
                return True
            else:
                logger.warning(
                    f"Failed to remove collaborator '{username}' from '{repo_name}' "
                    f"(status {resp.status_code})"
                )
                return False

        except httpx.HTTPError as e:
            logger.warning(
                f"Failed to remove collaborator '{username}' from '{repo_name}': {e}"
            )
            return False

    async def _get_oidc_source_id(
        self, provider_name: str = "Keycloak"
    ) -> Optional[int]:
        """Return the integer ID of the named OIDC auth source, or None.

        Used when pre-provisioning users via ensure_user so Gitea matches
        them to the correct auth source on subsequent OIDC login (instead
        of creating a duplicate local user).

        Strategy:
            1. Try /api/v1/admin/auths (Gitea 1.23+). If it returns the
               source, use the reported ID.
            2. If that API is unavailable (404 on Gitea 1.22), probe the
               OAuth redirect endpoint /user/oauth2/{provider_name}. A
               307/302 means the source is registered; 500 means it is
               not. On success, return 1 by convention — the dev compose
               entrypoint always creates Keycloak as the first auth source.
            3. Return None if the source is not yet registered so callers
               skip pre-provisioning rather than create broken records.
        """
        if not self._initialized:
            return None
        client = self._get_client()
        try:
            resp = await client.get(f"{self._url}/api/v1/admin/auths")
            if resp.status_code == 200:
                for src in resp.json():
                    if src.get("name") == provider_name:
                        return src.get("id")
                # API works but source not registered yet
                return None
        except httpx.HTTPError:
            pass

        # Fallback for Gitea <1.23: readiness probe via OAuth redirect.
        try:
            resp = await client.get(
                f"{self._url}/user/oauth2/{provider_name}",
                follow_redirects=False,
            )
            if resp.status_code in (302, 307):
                return 1
        except httpx.HTTPError:
            pass
        return None

    async def _find_user_by_login(self, username: str) -> Optional[dict]:
        """Fetch a Gitea user record by login (username). Returns full dict
        including source_id and login_name, or None if not found.
        """
        if not self._initialized or not username:
            return None
        client = self._get_client()
        url = f"{self._url}/api/v1/users/{validate_gitea_name(username, kind='user')}"
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
        except httpx.HTTPError:
            pass
        return None

    async def _repair_user_source(
        self,
        username: str,
        source_id: int,
        login_name: str,
    ) -> bool:
        """Relink an existing Gitea user to the given OIDC source_id.

        Heals users that were created with the wrong source_id (e.g.
        source_id=0 because the OIDC auth source wasn't registered when
        ensure_user first fired). Without this, Gitea's OIDC login sees
        an unlinked local user with matching login_name and prompts for
        password-based "Link to Existing Account" instead of auto-linking.
        """
        if not self._initialized or not username:
            return False
        client = self._get_client()
        user = validate_gitea_name(username, kind="user")
        try:
            resp = await client.patch(
                f"{self._url}/api/v1/admin/users/{user}",
                json={
                    "source_id": source_id,
                    "login_name": login_name,
                },
            )
            if resp.status_code in (200, 201):
                logger.info(
                    f"Relinked Gitea user '{username}' to source_id={source_id}"
                )
                return True
            logger.warning(
                f"Failed to relink user '{username}' to source_id={source_id} "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return False
        except httpx.HTTPError as e:
            logger.warning(f"Failed to relink user '{username}': {e}")
            return False

    async def ensure_user(
        self,
        email: str,
        username: str,
        full_name: Optional[str] = None,
        sub: Optional[str] = None,
        provider_name: str = "Keycloak",
    ) -> Optional[str]:
        """Idempotently pre-provision a Gitea user linked to the OIDC source.

        Closes the race where the orchestrator grants repo access to a user
        who has logged into cockpit via Keycloak but has not yet logged into
        Gitea directly (so Gitea's OIDC auto-registration hasn't fired).

        The created user has source_id pointing at the Keycloak auth source
        AND login_name set to the Keycloak subject (sub) UUID — that's the
        field Gitea matches on during OIDC login, so the user is linked to
        this pre-provisioned row instead of getting a duplicate account.

        Args:
            email: User's email address (must match Keycloak claim).
            username: Gitea username — should match Keycloak preferred_username
                so the login label is recognizable.
            full_name: Optional display name.
            sub: Keycloak subject identifier — used as Gitea's login_name so
                OIDC login resolves to this account. Strongly recommended;
                falling back to username risks duplicate accounts on first
                direct Gitea login.
            provider_name: Gitea auth source name (default: "Keycloak").

        Returns:
            The Gitea username on success (existing or newly created), None
            on failure.
        """
        if not self._initialized or not email or not username:
            return None

        source_id = await self._get_oidc_source_id(provider_name)
        # Gitea's OIDC login matches existing users by login_name == sub.
        # Use sub when provided, fall back to username (creates duplicate
        # risk, but better than nothing for non-OIDC providers).
        login_name = sub or username

        existing_login = await self.find_user_by_email(email)
        if existing_login:
            # Heal users created before the OIDC source was registered
            # (source_id=0) OR pointing at a stale sub (login_name mismatch).
            # Without this, Gitea's OIDC login shows "Link to Existing
            # Account" instead of linking silently.
            if source_id:
                detail = await self._find_user_by_login(existing_login)
                if detail and (
                    detail.get("source_id") != source_id
                    or detail.get("login_name") != login_name
                ):
                    await self._repair_user_source(
                        existing_login, source_id, login_name
                    )
            return existing_login

        if not source_id:
            # OIDC source not yet registered in Gitea (likely the compose
            # entrypoint's `gitea admin auth add-oauth` hasn't run yet).
            # Creating a user with source_id=0 would produce a local
            # account that can't be auto-linked by subsequent OIDC login,
            # so skip. The grant_user_repo_access caller will retry later
            # when either the source registers or the user visits Gitea
            # directly (triggering Gitea's own OIDC auto-registration).
            logger.info(
                f"Skipping Gitea pre-provision for '{email}' — "
                f"OIDC auth source '{provider_name}' not registered yet"
            )
            return None

        client = self._get_client()
        # OIDC users never password-authenticate against Gitea — we still
        # need to set *something* since the admin API requires it. Random
        # URL-safe token, never persisted anywhere readable.
        placeholder = secrets.token_urlsafe(32)

        try:
            resp = await client.post(
                f"{self._url}/api/v1/admin/users",
                json={
                    "username": username,
                    "login_name": login_name,
                    "email": email,
                    "full_name": full_name or username,
                    "password": placeholder,
                    "must_change_password": False,
                    "send_notify": False,
                    "source_id": source_id,
                },
            )
            if resp.status_code in (200, 201):
                logger.info(
                    f"Pre-provisioned Gitea user '{username}' "
                    f"(email={email}, source_id={source_id})"
                )
                return username
            if resp.status_code == 422:
                # Name or email collision — user exists under a slightly
                # different identity; leave lookup to the caller.
                logger.debug(
                    f"Gitea rejected ensure_user for '{username}' (422 — "
                    f"likely username/email collision)"
                )
                return None
            logger.warning(
                f"Failed to pre-provision Gitea user '{username}' "
                f"(status {resp.status_code}): {resp.text[:200]}"
            )
            return None
        except httpx.HTTPError as e:
            logger.warning(f"Failed to pre-provision Gitea user '{username}': {e}")
            return None

    async def grant_user_repo_access(
        self,
        email: str,
        repo_name: str,
        permission: str = "read",
        username: Optional[str] = None,
        full_name: Optional[str] = None,
        sub: Optional[str] = None,
    ) -> bool:
        """Grant a user access to a repository by email.

        Resolves the user's Gitea account by email, then adds them as a
        collaborator. If the user has no Gitea account yet and ``username``
        is provided, pre-provisions the account via ensure_user so the
        grant can proceed — this closes the race where a thread is created
        before the user's first Gitea OIDC login.

        Args:
            email: User's email address.
            repo_name: Repository name.
            permission: Access level — "read", "write", or "admin".
            username: Gitea username (from Keycloak preferred_username) used
                as a fallback to create the account if missing.
            full_name: Optional display name for the created account.
            sub: Keycloak subject — used as Gitea login_name so subsequent
                OIDC login links to this pre-provisioned account.

        Returns:
            True if granted, False if user not found/creatable or op failed.
        """
        if not self._initialized:
            return False

        resolved = await self.find_user_by_email(email)
        if not resolved and username:
            resolved = await self.ensure_user(email, username, full_name, sub=sub)
        if not resolved:
            logger.debug(
                f"No Gitea account for '{email}', skipping access grant on '{repo_name}'"
            )
            return False

        return await self.add_collaborator(repo_name, resolved, permission)

    async def revoke_user_repo_access(self, email: str, repo_name: str) -> bool:
        """Revoke a user's access to a repository by email.

        Resolves the user's Gitea account by email, then removes them as
        a collaborator. Skips gracefully if user has no Gitea account.

        Args:
            email: User's email address.
            repo_name: Repository name.

        Returns:
            True if revoked, False if user not found or operation failed.
        """
        if not self._initialized:
            return False

        username = await self.find_user_by_email(email)
        if not username:
            return True  # No account = no access to revoke

        return await self.remove_collaborator(repo_name, username)

    async def close(self) -> None:
        """Close the httpx client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
