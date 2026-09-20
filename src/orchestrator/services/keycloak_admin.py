"""Keycloak Admin API client for project group sync.

Syncs project membership to Keycloak groups so that downstream OIDC-connected
services (Nextcloud, Gitea) can use group claims for access control.

Gracefully degrades — if Keycloak is unavailable or unconfigured, all methods
log warnings and the system continues without group sync.
"""

import asyncio
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class KeycloakGroupSync:
    """Manages Keycloak groups corresponding to SRW projects.

    Reads configuration from environment variables:
        KEYCLOAK_URL: Base URL of the Keycloak instance (default: http://localhost:8180)
        KEYCLOAK_REALM: Realm name (default: srw)
        KEYCLOAK_ADMIN_USER: Admin username for Keycloak Admin API
        KEYCLOAK_ADMIN_PASSWORD: Admin password for Keycloak Admin API

    Group naming convention: project-{project_id}
    """

    def __init__(self) -> None:
        # python-keycloak joins relative endpoints with urljoin. A context
        # path without the trailing slash would be replaced (e.g. /identity).
        self._server_url = (
            os.getenv("KEYCLOAK_URL", "http://localhost:8180").rstrip("/") + "/"
        )
        self._realm = os.getenv("KEYCLOAK_REALM", "srw")
        self._admin_user = os.getenv("KEYCLOAK_ADMIN_USER", "")
        self._admin_password = os.getenv("KEYCLOAK_ADMIN_PASSWORD", "")
        self._initialized = False
        self._admin = None  # KeycloakAdmin instance

    @property
    def is_configured(self) -> bool:
        """True if admin credentials are set."""
        return bool(self._admin_user and self._admin_password)

    @property
    def is_initialized(self) -> bool:
        """True if Keycloak Admin API connection is verified."""
        return self._initialized

    async def ensure_initialized(self) -> bool:
        """Initialize KeycloakAdmin connection.

        Uses admin username/password to authenticate against the master realm,
        then targets the configured realm for group operations.

        Returns:
            True if Keycloak is ready, False if unavailable or setup failed.
        """
        if not self.is_configured:
            logger.info(
                "Keycloak group sync not configured "
                "(KEYCLOAK_ADMIN_USER/PASSWORD not set)"
            )
            return False

        try:
            result = await asyncio.to_thread(self._init_admin_sync)
            if result:
                self._initialized = True
                logger.info("Keycloak group sync initialized")
            return result
        except Exception as e:
            logger.warning(f"Keycloak group sync init failed: {e}")
            return False

    def _init_admin_sync(self) -> bool:
        """Synchronous initialization (runs in thread)."""
        from keycloak import KeycloakAdmin

        self._admin = KeycloakAdmin(
            server_url=self._server_url,
            username=self._admin_user,
            password=self._admin_password,
            realm_name=self._realm,
            user_realm_name="master",
        )
        # Verify connectivity by listing realm roles
        self._admin.get_realm_roles()
        return True

    def _group_name(self, project_id: str) -> str:
        """Get the Keycloak group name for a project."""
        return f"project-{project_id}"

    # =========================================================================
    # Group Operations (all fire-and-forget with error logging)
    # =========================================================================

    async def ensure_project_group(
        self, project_id: str, project_name: str
    ) -> Optional[str]:
        """Create a Keycloak group for a project if it doesn't exist.

        Args:
            project_id: Project UUID
            project_name: Human-readable project name (for group attributes)

        Returns:
            Keycloak group ID, or None on failure.
        """
        if not self._initialized:
            return None
        try:
            return await asyncio.to_thread(
                self._ensure_project_group_sync, project_id, project_name
            )
        except Exception as e:
            logger.warning(f"Failed to ensure group for project {project_id}: {e}")
            return None

    def _ensure_project_group_sync(
        self, project_id: str, project_name: str
    ) -> Optional[str]:
        group_name = self._group_name(project_id)

        # Check if group already exists
        existing = self._admin.get_groups(query={"search": group_name, "exact": True})
        for g in existing:
            if g["name"] == group_name:
                return g["id"]

        # Create the group
        group_id = self._admin.create_group({"name": group_name})
        logger.info(
            f"Created Keycloak group '{group_name}' for project '{project_name}'"
        )
        return group_id

    async def add_user_to_project_group(
        self, keycloak_sub: str, project_id: str
    ) -> bool:
        """Add a user to a project's Keycloak group.

        Args:
            keycloak_sub: Keycloak user ID (the 'sub' claim)
            project_id: Project UUID

        Returns:
            True if added, False on failure.
        """
        if not self._initialized:
            return False
        try:
            return await asyncio.to_thread(
                self._add_user_to_group_sync, keycloak_sub, project_id
            )
        except Exception as e:
            logger.warning(
                f"Failed to add user {keycloak_sub[:8]}... to project group {project_id}: {e}"
            )
            return False

    def _add_user_to_group_sync(self, keycloak_sub: str, project_id: str) -> bool:
        group_name = self._group_name(project_id)
        group_id = self._find_group_id(group_name)
        if not group_id:
            logger.warning(f"Group '{group_name}' not found, cannot add user")
            return False

        self._admin.group_user_add(user_id=keycloak_sub, group_id=group_id)
        logger.debug(f"Added user {keycloak_sub[:8]}... to group '{group_name}'")
        return True

    async def remove_user_from_project_group(
        self, keycloak_sub: str, project_id: str
    ) -> bool:
        """Remove a user from a project's Keycloak group.

        Args:
            keycloak_sub: Keycloak user ID (the 'sub' claim)
            project_id: Project UUID

        Returns:
            True if removed, False on failure.
        """
        if not self._initialized:
            return False
        try:
            return await asyncio.to_thread(
                self._remove_user_from_group_sync, keycloak_sub, project_id
            )
        except Exception as e:
            logger.warning(
                f"Failed to remove user {keycloak_sub[:8]}... from project group {project_id}: {e}"
            )
            return False

    def _remove_user_from_group_sync(self, keycloak_sub: str, project_id: str) -> bool:
        group_name = self._group_name(project_id)
        group_id = self._find_group_id(group_name)
        if not group_id:
            return True  # Group doesn't exist, nothing to remove from

        self._admin.group_user_remove(user_id=keycloak_sub, group_id=group_id)
        logger.debug(f"Removed user {keycloak_sub[:8]}... from group '{group_name}'")
        return True

    async def delete_project_group(self, project_id: str) -> bool:
        """Delete a project's Keycloak group.

        Args:
            project_id: Project UUID

        Returns:
            True if deleted, False on failure.
        """
        if not self._initialized:
            return False
        try:
            return await asyncio.to_thread(self._delete_group_sync, project_id)
        except Exception as e:
            logger.warning(f"Failed to delete group for project {project_id}: {e}")
            return False

    def _delete_group_sync(self, project_id: str) -> bool:
        group_name = self._group_name(project_id)
        group_id = self._find_group_id(group_name)
        if not group_id:
            return True  # Already gone

        self._admin.delete_group(group_id=group_id)
        logger.info(f"Deleted Keycloak group '{group_name}'")
        return True

    def _find_group_id(self, group_name: str) -> Optional[str]:
        """Find a group's Keycloak ID by name."""
        groups = self._admin.get_groups(query={"search": group_name, "exact": True})
        for g in groups:
            if g["name"] == group_name:
                return g["id"]
        return None
