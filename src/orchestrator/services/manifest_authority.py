"""Live scope authorization; authored names never confer permissions."""

from uuid import UUID

from fastapi import HTTPException

from orchestrator.security.access import (
    _denied,
    _role_satisfies,
    _scope_permits_project,
    mcp_scope_project_id,
)
from orchestrator.services.connector_secrets import (
    CATALOG_SECRET_DETAIL,
    FOREIGN_CONNECTOR_SECRET_DETAIL,
    connector_policy_authorizes,
    is_connector_secret_name,
    is_own_connector_secret,
)
from orchestrator.services.project_status import project_is_archived


class ManifestAuthority:
    def __init__(self, db, user, *, request=None):
        self.db, self.user, self.request = db, user, request
        self.account = {"kind": "Account", "name": str(user["id"])}
        self.new_projects = set()

    async def deny(self, detail):
        raise await _denied(
            self.request,
            self.db,
            self.user,
            resource_type="manifest",
            resource_id=None,
            detail=detail,
        )

    async def scope(self, scope=None, *, write=False):
        scope = dict(scope or self.account)
        if scope["kind"] == "Account":
            if scope["name"] in ("me", "personal"):
                scope["name"] = self.account["name"]
            if not _scope_permits_project(self.user, None):
                await self.deny(
                    "Account resources are outside this token's project scope."
                )
            if scope["name"] != self.account["name"] and not self.user.get("is_admin"):
                await self.deny("Account resource belongs to a different user.")
            try:
                UUID(scope["name"])
            except ValueError:
                raise HTTPException(
                    422, "Live Account scopes require a user UUID or 'me'."
                ) from None
        elif scope["kind"] == "Catalog":
            if scope["name"] != "shared":
                raise HTTPException(
                    422, "This installation provides the 'shared' Catalog."
                )
            if write and (
                not self.user.get("is_admin")
                or not _scope_permits_project(self.user, None)
            ):
                await self.deny(
                    "Publishing shared resources requires an unrestricted administrator."
                )
        else:
            project_id = scope["name"]
            if not _scope_permits_project(self.user, project_id):
                await self.deny("Project resource is outside this token's scope.")
            if project_id in self.new_projects:
                return scope
            project = await self.db.get_project(project_id)
            if not project:
                raise HTTPException(
                    404,
                    "Project scope does not exist; use its UUID or include its Project manifest.",
                )
            if not self.user.get("is_admin"):
                role = await self.db.get_user_role_in_project(
                    project_id, str(self.user["id"])
                )
                if not _role_satisfies(role, "editor" if write else "viewer"):
                    await self.deny(
                        "Project membership does not permit this resource operation."
                    )
            if write and project_is_archived(project):
                raise HTTPException(
                    409,
                    "Archived projects cannot accept configuration changes or new work.",
                )
        return scope

    async def resource(self, row, *, write=False):
        if row["kind"] == "Expert" and row.get("linked_id") and not write:
            token_project = mcp_scope_project_id(self.user)
            if token_project:
                await self.scope({"kind": "Project", "name": str(token_project)})
                project_ids = [str(token_project)]
            else:
                projects = await self.db.get_projects_for_user(str(self.user["id"]))
                project_ids = [str(project["id"]) for project in projects]
            expert = await self.db.get_expert_visible_by_id(
                str(row["linked_id"]),
                user_id=str(self.user["id"]),
                project_ids=project_ids,
                is_admin=bool(self.user.get("is_admin")),
            )
            if not expert:
                await self.deny(
                    "The selected Expert is no longer visible to this caller."
                )
            if token_project and not expert.get("is_global"):
                link = await self.db.get_project_expert_link(
                    project_id=str(token_project), expert_id=str(row["linked_id"])
                )
                if not link:
                    await self.deny(
                        "The selected Expert is outside this token's Project scope."
                    )
            return
        # Projects retain their actual membership authority, even when the
        # portable Project document is in its creator's Account scope.
        if row["kind"] == "Project" and row.get("linked_id"):
            await self.scope(
                {"kind": "Project", "name": str(row["linked_id"])}, write=write
            )
        else:
            await self.scope(row["document"]["metadata"]["scope"], write=write)

    async def secret(self, scope, *, name=None):
        """Authority to attach the secret ``name`` in ``scope`` to a process.

        A Connector's own secret (``connector-<32 hex>``) is never lent this
        way: only its Connector uses it (:meth:`connector_secret`), so a
        reference from another resource cannot bypass the connector policy
        or carry a credential the connector's driver never forwards.
        """
        if scope["kind"] == "Catalog":
            await self.deny(CATALOG_SECRET_DETAIL)
        if is_connector_secret_name(name):
            await self.deny(FOREIGN_CONNECTOR_SECRET_DETAIL)
        # Reading the manifest is weaker than attaching credentials to a process.
        return await self.scope(scope, write=True)

    async def connector_secret(self, ref, connector, *, project_ids=()):
        """Authority to use a linked Connector's own credentials (decision 11).

        A connector shared with other users (public, or linked to their
        project) lends its creator's credentials to their executions, as a
        datasource always did.  So when ``ref`` names ``connector``'s own
        resource secret, in the Connector's own scope, the connector policy
        decides: the secret is usable by work in ``project_ids`` the policy
        authorizes this caller to attach the connector to, although the
        caller cannot write the secret's scope.  Otherwise the ordinary
        rule (:meth:`secret`) decides, so the owner and an administrator
        keep their access.  A Catalog secret is refused either way.
        """
        scope = ref["scope"]
        if scope["kind"] == "Catalog":
            await self.deny(CATALOG_SECRET_DETAIL)
        own = is_own_connector_secret(ref, connector)
        if own and await connector_policy_authorizes(
            self.db, self.user, connector["linked_id"], project_ids
        ):
            return dict(scope)
        return await self.secret(scope, name=None if own else ref.get("name"))
