"""Live scope authorization; authored names never confer permissions."""

from uuid import UUID

from fastapi import HTTPException

from orchestrator.security.access import (
    _denied,
    _role_satisfies,
    _scope_permits_project,
    mcp_scope_project_id,
    user_can_access_datasource,
)
from orchestrator.services.connector_secrets import (
    CATALOG_SECRET_DETAIL,
    FOREIGN_CONNECTOR_SECRET_DETAIL,
    connector_policy_authorizes,
    is_connector_secret_name,
    is_own_connector_secret,
)
from orchestrator.services.datasource_policy import GENERIC_UNAVAILABLE_DETAIL
from orchestrator.services.project_status import project_is_archived


class ManifestAuthority:
    def __init__(self, db, user, *, request=None):
        self.db, self.user, self.request = db, user, request
        self.account = {"kind": "Account", "name": str(user["id"])}
        self.new_projects = set()
        self._connector_policies = {}

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
        if row["kind"] == "Connector" and row.get("linked_id") and not write:
            # A datasource's Connector is visible by the connector policy, as
            # its datasource is to the link API: to its owner, an
            # administrator, everyone when public, and the members of a project
            # it is linked to; never as a Catalog resource (a datasource's
            # Connector is never one). Writes stay with its scope (and the
            # store refuses them: the datasource API writes it).
            datasource = (
                None
                if row.get("scope_kind") == "Catalog"
                else await self.db.get_datasource(str(row["linked_id"]))
            )
            if not datasource or not (
                datasource.get("is_global")
                or await user_can_access_datasource(self.user, self.db, datasource)
            ):
                await self.deny("The selected connector is not visible to this caller.")
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

    async def connector_secret(self, ref, connector, *, project_ids=(), edit=False):
        """Authority to use a linked Connector's own credentials (decision 11).

        A connector shared with other users (public, or linked to their
        project) lends its creator's credentials to their executions, as a
        datasource always did.  So when ``ref`` names ``connector``'s own
        resource secret, in the Connector's own scope, the connector policy
        is the authority, and the only one: the secret is usable by work in
        ``project_ids`` the policy authorizes this caller to attach the
        connector to, whoever can write the secret's scope.  The owner's
        ``scope_mode=projects`` connector outside its projects and a
        project's knowledge base outside its project are refused, as their
        delivery is.

        ``edit`` is a manifest apply of the Connector's own document: the
        store refuses that edit itself (a datasource's Connector is written
        from its row), and the ordinary rule (:meth:`secret`) lets the
        caller who could write it reach that refusal.  A reference to any
        other secret is :meth:`secret`'s, which refuses a connector's; a
        Catalog secret is refused either way.
        """
        scope = ref["scope"]
        if scope["kind"] == "Catalog":
            await self.deny(CATALOG_SECRET_DETAIL)
        if not is_own_connector_secret(ref, connector):
            return await self.secret(scope, name=ref.get("name"))
        if await self._connector_policy(connector["linked_id"], project_ids):
            return dict(scope)
        if edit:
            return await self.secret(scope)
        return await self.deny(GENERIC_UNAVAILABLE_DETAIL)

    async def _connector_policy(self, connector_id, project_ids):
        """The connector policy's answer for this caller, once per connector
        and set of projects."""
        key = (str(connector_id), tuple(sorted(str(p) for p in project_ids)))
        if key not in self._connector_policies:
            self._connector_policies[key] = await connector_policy_authorizes(
                self.db, self.user, connector_id, project_ids
            )
        return self._connector_policies[key]
