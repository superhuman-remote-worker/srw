"""Project lifecycle, membership, repository, connector-link and promotion work.

The authorization decisions stay with the router's injected gates; what lives
here is what happens *after* a gate has said yes. Two operations take a bound
callable instead of a gate because their escalation is conditional on the
request body and must keep its position in the existing check order:
``update_project`` (admin-only ``network_tier``) and the pair
``resolve_datasource_unlink`` / ``unlink_datasource_from_project`` (project
owner required only when the caller is neither admin nor the connector's
owner). Moving either check earlier would change which status code a request
that trips two rules receives.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException

from dataclasses import dataclass

from orchestrator.database.postgres import (
    DatasourceCatalogCursorError,
    DatasourceProjectAuthorizationError,
)
from orchestrator.schemas.datasources import ProjectDatasourceSettings
from orchestrator.schemas.projects import (
    ExternalKnowledgeBase,
    ProjectCreate,
    ProjectMemberAdd,
    ProjectMemberUpdate,
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectUpdate,
    PromoteRequest,
)
from orchestrator.security.access import (
    mcp_scope_project_id,
    normalize_project_statuses,
    project_is_archived,
    project_status_filter_sql,
    redact_datasource,
    redact_datasources,
    redact_public_config_override,
    redact_repositories,
    redact_repository,
    restore_hidden_config_values,
    user_can_access_datasource,
)
from orchestrator.services import project_provisioning
from orchestrator.services.cloud import ProjectFolderHandle
from orchestrator.services.cloud.identity import (
    get_home_browser_url_cached,
    peek_home_browser_url,
)
from orchestrator.services.config_overrides import (
    validated_config_name as _validated_config_name,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    create_managed_repository,
    ensure_project_repository_authority,
    project_repository_access_mode,
    revoke_and_delete_managed_repository,
    rotate_project_repository_authority,
)
from orchestrator.services.officer_metadata import (
    thread_officer_meta as _thread_officer_meta,
)


@dataclass(frozen=True)
class ProjectDependencies:
    """Per-invocation collaborators for the project operations.

    Every singleton here (``store``, ``vector_db``, ``forge``,
    ``keycloak_groups``, ``main_cloud_router``) is rebound during the
    application's ``lifespan``; the factory that builds this dataclass runs per
    request, so nothing is captured at import time.

    ``with_validated_tool_overrides`` is the application's one write-boundary
    vocabulary gate for a ``tools`` block, shared with session and job create;
    it is injected rather than re-implemented so all three boundaries keep
    answering identically.
    """

    store: Any
    vector_db: Any
    forge: Any
    keycloak_groups: Any
    main_cloud_router: Any
    logger: Any
    provisioning: project_provisioning.ProjectProvisioningDependencies
    with_validated_tool_overrides: Callable[
        [dict[str, Any] | None], dict[str, Any] | None
    ]


def public_project(project: dict[str, Any]) -> dict[str, Any]:
    """A project row as it may leave the orchestrator.

    ``default_config_override`` is merged under every job in the project, so it
    can hold what a job override holds — a BYO ``llm.api_key``, capability keys
    in ``env_keys``, a mount's ``rclone_spec``, the ``workspace.remote``
    transport — and every project MEMBER reads this row. It leaves by the job
    API's policy (:func:`redact_public_config_override`); ``update_project``
    restores the hidden values when the redacted view is written back.

    A manifest-composed row (every project after ``migrate_projects``) also
    carries the Project ``manifest``, whose Expert ``layers`` hold that override
    and each link override verbatim, and whose team controller holds the
    officer's config; it leaves by the same policy. The document keeps its
    shape, so a reader comparing it to its resource still can.
    """
    out = dict(project)
    for key in ("default_config_override", "manifest"):
        if out.get(key) is not None:
            out[key] = redact_public_config_override(out[key])
    return out


# =============================================================================
# Project lifecycle
# =============================================================================


async def plan_external_kb(
    body: ExternalKnowledgeBase,
    *,
    caller: dict[str, Any] | None = None,
    project_id: str | None = None,
    dependencies: ProjectDependencies,
) -> project_provisioning.KbVaultPlan:
    """Resolve a request's external-vault block into one validated plan."""
    return await project_provisioning.plan_external_kb_vault(
        body,
        caller=caller,
        project_id=project_id,
        dependencies=dependencies.provisioning,
    )


async def create_project(
    body: ProjectCreate,
    *,
    user: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Create a new project with the requesting user as owner."""
    store = dependencies.store
    logger = dependencies.logger
    # H5: pre-fix, this endpoint had no auth at all and trusted body.user_id,
    # so any unauthenticated caller could create projects owned by anyone.
    # Admins keep the ability to create on behalf of others (legitimate
    # setup flow); regular users get bound to themselves.
    owner_id = body.user_id if user.get("is_admin") else str(user["id"])
    # A project's default_config_override is merged UNDER every job created in
    # the project (create_job), so an unvalidated tools block here is a
    # cross-principal escalation: the planter is not the runner, and the PDP
    # keys off the category name, so `tools.canvas: ["run_command"]` binds a
    # shell tool for jobs whose owner has no shell_tools grant. Validated on
    # WRITE only — no existing row is touched, and the field only reaches this
    # check when a caller explicitly sends it.
    validated_project_override = dependencies.with_validated_tool_overrides(
        body.default_config_override
    )
    # Same write-only rule for the project's default selector: create_job
    # copies it into jobs.config_name in the legacy compatibility branch, and
    # from there it reaches the pod entrypoint.
    validated_default_config_name = _validated_config_name(body.default_config_name)
    # Resolve the full vault target — URL, auth, forge, and for a connector
    # request every reason it may not be adopted — before creating the project,
    # so a rejected external-vault request cannot leave a half-created one.
    external_kb_plan = (
        await plan_external_kb(body.external_kb, caller=user, dependencies=dependencies)
        if body.external_kb is not None
        else None
    )
    try:
        project = await store.create_project(
            name=body.name,
            description=body.description,
            goal=body.goal,
            default_config_name=validated_default_config_name,
            default_config_override=validated_project_override,
        )

        # Add creator as owner
        await store.add_project_member(
            project_id=str(project["id"]),
            user_id=owner_id,
            role="owner",
        )

        # Create only the dedicated KB vault. Root jobs get isolated repositories
        # when they are created; project membership never selects a shared
        # workspace. See project_jobs_repo_retirement.md.
        if external_kb_plan is not None:
            await project_provisioning.provision_external_project_knowledge_repo(
                project,
                owner_id,
                external_kb_plan,
                dependencies=dependencies.provisioning,
            )
        elif dependencies.forge.is_initialized:
            # The vault is written server-side and never cloned into a workspace.
            # A Gitea hiccup remains non-fatal to project creation, but no new
            # project falls back to a jobs repo because none is provisioned.
            try:
                await project_provisioning.provision_project_knowledge_repo(
                    project, owner_id, dependencies=dependencies.provisioning
                )
            except Exception as e:
                logger.warning(
                    f"Failed to provision knowledge repo for project "
                    f"{project['id']}: {e}"
                )

        # Provision Keycloak group + main-cloud Space + WebDAV datasource,
        # then sync the creator into both the Keycloak and LibreGraph groups.
        # The dual group write is intentional: Keycloak carries the `groups`
        # token claim (durable — OpenCloud's proxy reconciles LibreGraph
        # memberships from it on every login), while the direct backend add
        # makes the Space visible in the creator's currently-active OpenCloud
        # session without waiting for them to re-authenticate.
        project = await project_provisioning.ensure_project_cloud_resources(
            project, dependencies=dependencies.provisioning
        )

        return public_project(project)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def attach_project_knowledge_repository(
    project_id: str,
    body: ExternalKnowledgeBase,
    *,
    caller: dict[str, Any],
    project: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Attach an external GitHub live vault to an existing repo-less project.

    Replacing or migrating an existing knowledge-role repository is
    deliberately not implicit: v1 has no approved note/history migration.
    """
    existing = await dependencies.store.get_project_repositories(
        project_id, role="knowledge"
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                "Project already has a knowledge repository; migration/replacement "
                "is not supported by this attach path"
            ),
        )
    plan = await plan_external_kb(
        body, caller=caller, project_id=project_id, dependencies=dependencies
    )
    try:
        (
            repository,
            datasource,
        ) = await project_provisioning.provision_external_project_knowledge_repo(
            project,
            str(caller["id"]) if caller.get("id") else None,
            plan,
            dependencies=dependencies.provisioning,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail="Failed to attach external knowledge repository"
        ) from exc
    return {
        "status": "attached",
        "repository": redact_repository(repository),
        "datasource": redact_datasource(datasource),
    }


async def list_projects(
    *,
    caller: dict[str, Any],
    user_id: str | None,
    status: list[str] | None,
    dependencies: ProjectDependencies,
) -> list[dict[str, Any]]:
    """List projects visible to the caller.

    Visibility model (G2):
        * Admins see the full list, optionally narrowed by ``?user_id=`` or
          by an MCP ``project:<uuid>`` token scope.
        * Non-admins see only the projects they're a member of
          (``get_projects_for_user(caller)``), narrowed by any MCP scope.
        * A non-admin passing ``?user_id=`` for anyone other than themselves
          is rejected (403). Self-query is allowed but redundant.

    Lifecycle (knowledge-base/knowledge/features/project_and_job_list_filtering.md
    §4.2): archived projects are excluded by default on BOTH branches. The
    server does this, not the client — a filter applied in Angular still pays
    for the rows in Postgres and leaves every other consumer unprotected.
    The old admin-branch ``status != 'deleted'`` predicate was dead code:
    ``valid_project_status`` has no such value (deletion is a hard row
    delete), so it never excluded anything.

    ``get_projects_for_user`` keeps its LIMIT 100. With archived excluded that
    ceiling stops being reachable for realistic accounts; if it ever is, the
    projects grid needs paging too (out of scope, phase 1).
    """
    store = dependencies.store
    is_admin = bool(caller.get("is_admin"))
    scope_pid = mcp_scope_project_id(caller)
    statuses = normalize_project_statuses(status)

    if user_id is not None and not is_admin and str(user_id) != str(caller["id"]):
        raise HTTPException(
            status_code=403,
            detail="Not authorized to query other users' projects",
        )

    try:
        if is_admin and user_id is None:
            async with store.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM projects WHERE "
                    f"{project_status_filter_sql('$1')} "
                    "ORDER BY updated_at DESC LIMIT 100",
                    statuses,
                )
            from orchestrator.services.manifest_projects import hydrate_project_row

            projects = [await hydrate_project_row(store, row) for row in rows]
        elif user_id is not None:
            projects = await store.get_projects_for_user(user_id, statuses=statuses)
        else:
            projects = await store.get_projects_for_user(
                str(caller["id"]), statuses=statuses
            )

        if scope_pid:
            projects = [p for p in projects if str(p.get("id", "")) == str(scope_pid)]

        return [public_project(p) for p in projects]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_project(
    project_id: str,
    *,
    project: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Get a single project by ID.

    The warm path is pure Postgres — cloud/Keycloak reconciliation and
    identity resolution run as throttled background repairs, never on the
    request (knowledge-base/knowledge/issues/project_page_open_blocks_on_cloud_heal.md
    measured 2.3-5s per page open when they were inline).
    """
    store = dependencies.store
    logger = dependencies.logger

    # Lazy-heal (folder for pre-fix projects + member-group drift repair) —
    # fire-and-forget with a per-project cooldown. A legacy project without a
    # folder handle serves cloud_storage_url=None on this first open and gets
    # the deep-link once the background heal lands; the cockpit hides the
    # button for None. create_project keeps its blocking call — creation is
    # the primary provisioning path.
    project_provisioning.fire_background_repair(
        f"project-heal:{project_id}",
        project_provisioning.ensure_project_cloud_resources(
            project, dependencies=dependencies.provisioning
        ),
        dependencies=dependencies.provisioning,
    )

    # Compute cloud_storage_url for cockpit deep-links.
    #
    # Decorative-only: this whole block produces one optional URL for a button
    # the cockpit hides when it is None. It must never fail the request — a
    # pre-0186 row (provider name stamped, no backend-instance UUID) has no
    # resolvable installation authority, and 500ing the project page over a
    # missing deep-link took every cloud-backed project on dev offline behind a
    # misleading "You don't have access to that project" toast. The row still
    # refuses *effects* through the raising `for_project` at the mutation call
    # sites, which is the behaviour we want.
    project["cloud_storage_url"] = None
    backend = dependencies.main_cloud_router.for_project_optional(project)
    if backend is not None and backend.is_initialized:
        if project.get("is_default"):
            # Default projects piggyback on the owner's personal home Space —
            # the deep-link must resolve to that home, not to a project Space
            # (which we no longer provision for defaults — see
            # `ensure_project_cloud_resources`). A stale handle from older
            # deployments is intentionally ignored here.
            try:
                members = await store.get_project_members(project_id)
                owner = next((m for m in members if m.get("role") == "owner"), None)
                if owner and owner.get("email"):
                    url = await peek_home_browser_url(
                        store, str(owner["user_id"]), backend.backend_id
                    )
                    if url:
                        project["cloud_storage_url"] = url
                    else:
                        # Cache miss: resolve+persist off the request path;
                        # this open falls back to the generic home URL.
                        owner_user = {
                            "id": owner["user_id"],
                            "email": owner.get("email"),
                            "display_name": owner.get("display_name"),
                        }
                        project_provisioning.fire_background_repair(
                            f"home-url:{owner['user_id']}:{backend.backend_id}",
                            get_home_browser_url_cached(store, owner_user, backend),
                            dependencies=dependencies.provisioning,
                        )
            except Exception as e:
                logger.warning(
                    f"Failed to resolve user-home URL for default project "
                    f"{project_id}: {e}"
                )
            if not project["cloud_storage_url"]:
                project["cloud_storage_url"] = backend.get_default_home_browser_url()
        else:
            handle_str = project.get("main_cloud_folder_handle")
            legacy_folder_id = project.get("nextcloud_folder_id")
            if handle_str or legacy_folder_id:
                handle = ProjectFolderHandle.from_db(
                    handle_str or str(legacy_folder_id),
                    backend=project.get("main_cloud_backend") or backend.backend_id,
                )
                # Legacy Nextcloud handles were backfilled without vendor_meta;
                # re-attach the mountpoint from the project name so the URL
                # builder has what it needs.
                if not handle.vendor_meta.get("mountpoint"):
                    handle = ProjectFolderHandle(
                        backend=handle.backend,
                        native_id=handle.native_id,
                        vendor_meta={
                            **handle.vendor_meta,
                            "mountpoint": project["name"],
                        },
                    )
                project["cloud_storage_url"] = backend.get_project_folder_browser_url(
                    handle
                )

    return public_project(project)


async def quiesce_archived_project(
    project_id: str, *, dependencies: ProjectDependencies
) -> dict[str, Any]:
    """Stop a project's unattended machinery after it is archived (§4.5).

    Never refuses and never raises: across GitHub, GitLab, Jira, Asana, Linear
    and Slack, no product refuses an archive because children are in flight —
    refusing makes archive un-completable exactly when you most want it, i.e.
    when something is wedged and you want it to stop mattering. So the archive
    has already been written by the time we get here; each step below is
    best-effort and reports what it managed to do.

    Three children, matching Slack's deterministic-deactivation tier rather
    than Jira's "your automation rules just start failing":

    * a *running* loop is paused (a paused one is left alone),
    * the officer is held — the same one key the maintenance-hold endpoint
      stamps, with NO ``thread_id``, which is what stops the watchdog's
      stale-hold self-heal from releasing it,
    * jobs the dispatcher has not claimed yet are parked.

    ``processing`` jobs are deliberately untouched: in-flight work, not new
    work. Unarchive does NOT undo any of this — implicitly re-animating
    automation is where the surprises live.
    """
    store = dependencies.store
    logger = dependencies.logger

    report: dict[str, Any] = {
        "loop_paused": False,
        "officer_held": False,
        "jobs_parked": 0,
    }

    try:
        loop = await store.get_active_project_loop(project_id)
        if loop and loop.get("status") == "running":
            await store.update_project_loop(str(loop["id"]), status="paused")
            report["loop_paused"] = True
    except Exception:
        logger.exception(
            "archive %s: failed to pause the project loop (archive stands)",
            project_id,
        )

    try:
        officer = await store.get_officer_thread_for_project(project_id)
        if officer and not _thread_officer_meta(officer).get("hold"):
            await store.set_project_officer_hold(
                project_id,
                expected_thread_id=str(officer["id"]),
                hold={
                    "kind": "project_archived",
                    "since": datetime.now(timezone.utc).isoformat(),
                    "note": "The project was archived.",
                },
                route_reason="officer_hold",
            )
            report["officer_held"] = True
    except Exception:
        logger.exception(
            "archive %s: failed to hold the officer (archive stands)", project_id
        )

    try:
        report["jobs_parked"] = await store.park_project_jobs_for_archive(project_id)
    except Exception:
        logger.exception(
            "archive %s: failed to park pending jobs (archive stands)", project_id
        )

    logger.info(
        "archive %s: quiesced (loop_paused=%s officer_held=%s jobs_parked=%s)",
        project_id,
        report["loop_paused"],
        report["officer_held"],
        report["jobs_parked"],
    )
    return report


async def update_project(
    project_id: str,
    body: ProjectUpdate,
    *,
    project: dict[str, Any],
    escalate_admin: Callable[[], Awaitable[Any]],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Update a project. Caller must be a project owner or admin.

    ALLOW-listed for archived projects because it is the unarchive path — but
    **status-only** while archived (§4.3a). The handler is generic: it also
    covers ``name``, ``goal`` and ``default_config_override``, and that last
    one is merged under *every job in the project*, so leaving it open would
    stop "archived" meaning read-only in the way the shipped UI copy promises.
    The guard flag cannot express this — it fires before the body is
    inspected — so it is a body-level check here. Renaming an archived project
    is a legitimate want; it just needs an unarchive first, which is one click.

    Setting ``status='archived'`` additionally quiesces the project's children
    and reports what it stopped (§4.5).
    """
    store = dependencies.store
    logger = dependencies.logger

    kwargs = {k: v for k, v in body.model_dump().items() if v is not None}
    if not kwargs:
        raise HTTPException(status_code=400, detail="No fields to update")
    if project_is_archived(project) and set(kwargs) - {"status"}:
        # Refuse the WHOLE request rather than applying the status half and
        # dropping the rest: a partially-applied PATCH is the worst outcome
        # here, since the caller has no way to tell which half landed.
        raise HTTPException(
            status_code=409,
            detail=(
                "This project is archived and is read-only apart from its "
                "status. Unarchive it before editing anything else."
            ),
        )
    # Write-only, same rationale as create_project: this layer is merged under
    # every job in the project, so a tools block here escapes the runner's
    # grants. Only fires when the field is explicitly sent — the cockpit's
    # project-memory toggle re-submits the STORED override, so a project
    # already holding an invalid tools block surfaces it here instead of
    # quietly binding foreign tools on every job.
    if "default_config_override" in kwargs:
        # Reads serve the redacted view (public_project) and the cockpit writes
        # that view back whole, so without this, flipping one key would delete
        # every stored secret in the override.
        kwargs["default_config_override"] = restore_hidden_config_values(
            kwargs["default_config_override"], project.get("default_config_override")
        )
        kwargs["default_config_override"] = dependencies.with_validated_tool_overrides(
            kwargs["default_config_override"]
        )
    # Write-only for the same reason: a project already holding an unbootable
    # default surfaces it when a caller re-submits the field, never on a PATCH
    # that does not name it.
    if "default_config_name" in kwargs:
        kwargs["default_config_name"] = _validated_config_name(
            kwargs["default_config_name"]
        )
    # network_tier is admin-gated: a tier change widens what the project's
    # workspaces can reach at the pod-network layer (e.g. home-allowed
    # exposes the homelab LAN). Letting any project owner choose their own
    # tier defeats the operator-side control plane. See
    # knowledge-base/knowledge/features/workspace_network_isolation.md §3.
    if body.network_tier is not None:
        await escalate_admin()
    success = await store.update_project(project_id, **kwargs)
    if not success:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")

    # Sync cloud_storage_read_only to the project-scoped WebDAV datasource
    if body.cloud_storage_read_only is not None:
        try:
            project_ds = await store.list_project_datasources(project_id)
            for ds in project_ds:
                if ds["type"] == "webdav":
                    await store.link_datasource_to_project(
                        project_id=project_id,
                        datasource_id=str(ds["id"]),
                        read_only=body.cloud_storage_read_only,
                    )
                    break
        except Exception as e:
            logger.warning(
                f"Failed to sync cloud_storage_read_only to datasource "
                f"for project {project_id}: {e}"
            )

    # Archiving quiesces the children and says so (§4.5). Only on the
    # transition INTO archived: re-PATCHing an already-archived project must
    # not re-hold an officer the owner deliberately released. Unarchive
    # (status='active') resumes nothing — that stays explicit.
    archiving_now = str(kwargs.get("status") or "").lower() == "archived"
    if archiving_now and not project_is_archived(project):
        quiesced = await quiesce_archived_project(project_id, dependencies=dependencies)
        return {"status": "updated", "archived": True, **quiesced}

    return {"status": "updated"}


async def delete_project(
    project_id: str,
    *,
    project: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Delete a project. Caller must be a project owner or admin. Cannot delete default projects."""
    store = dependencies.store
    logger = dependencies.logger

    if project.get("is_default"):
        raise HTTPException(status_code=400, detail="Cannot delete a default project")

    # Clean up managed repos
    repos = await store.get_project_repositories(project_id)
    for repo in repos:
        if repo.get("is_managed"):
            if not await revoke_and_delete_managed_repository(
                store, dependencies.forge, repo["name"]
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Repository credential revocation is retryable",
                )

    # Clean up knowledge_index in vector DB (no FK cascade across databases)
    try:
        async with dependencies.vector_db.acquire() as conn:
            await conn.execute(
                "DELETE FROM knowledge_index WHERE project_id = $1", UUID(project_id)
            )
    except Exception as e:
        logger.warning(
            f"Failed to clean up knowledge_index for project {project_id}: {e}"
        )

    # Detach referencing rows that lack ON DELETE CASCADE/SET NULL
    uuid_val = UUID(project_id)
    async with store.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET project_id = NULL WHERE project_id = $1", uuid_val
        )
        await conn.execute(
            "UPDATE datasources SET project_id = NULL WHERE project_id = $1", uuid_val
        )

    success = await store.delete_project(project_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete project")

    # Clean up Keycloak group
    if dependencies.keycloak_groups.is_initialized:
        await dependencies.keycloak_groups.delete_project_group(project_id)

    # Clean up main-cloud project folder via the row's backend dispatch.
    #
    # The DB row is already gone by this point, so raising here would report a
    # delete that actually happened as a 500. An unresolvable installation
    # authority means we cannot say *which* installation holds the folder, and
    # deleting from a guessed one is unacceptable — so leave the folder in
    # place, say so, and let the delete stand.
    backend = dependencies.main_cloud_router.for_project_optional(project)
    handle_str = project.get("main_cloud_folder_handle") or (
        str(project["nextcloud_folder_id"])
        if project.get("nextcloud_folder_id")
        else None
    )
    if backend is None:
        if handle_str:
            logger.warning(
                "Project %s deleted, but its main-cloud folder (%s on provider "
                "%r) was left in place: no resolvable backend instance for the "
                "row. The folder needs manual cleanup.",
                project_id,
                handle_str,
                project.get("main_cloud_backend"),
            )
    elif handle_str and backend.is_initialized:
        try:
            handle = ProjectFolderHandle.from_db(
                handle_str,
                backend=project.get("main_cloud_backend") or backend.backend_id,
            )
            await backend.delete_project_folder(handle)
        except Exception as e:
            logger.warning(
                f"Failed to delete main-cloud folder for project {project_id}: {e}"
            )

    return {"status": "deleted"}


# =============================================================================
# Project members
# =============================================================================


async def list_project_members(
    project_id: str, *, dependencies: ProjectDependencies
) -> list[dict[str, Any]]:
    """List members of a project with user info."""
    return await dependencies.store.get_project_members(project_id)


async def add_project_member(
    project_id: str,
    body: ProjectMemberAdd,
    *,
    project: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Add a member to a project. Caller must be a project owner or admin."""
    store = dependencies.store
    logger = dependencies.logger

    # Resolve the cloud backend BEFORE writing the member row. The cloud group
    # add below is load-bearing (a member without it cannot reach the project's
    # files), so a row we cannot sync is a half-landed member: present in the
    # DB, invisible in the cloud, and reported to the caller as a 500 by the
    # blanket handler below. Refuse up front instead, with a diagnosis — a
    # pre-0186 row has no resolvable installation authority until it is
    # stamped, and we must not guess which installation to grant against.
    backend = dependencies.main_cloud_router.for_project_optional(project)
    if backend is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "This project's main-cloud installation authority is "
                "unresolved, so project membership cannot be granted on the "
                "cloud backend. The project row needs a "
                "main_cloud_backend_instance_id backfill."
            ),
        )

    try:
        result = await store.add_project_member(
            project_id=project_id,
            user_id=body.user_id,
            role=body.role,
        )

        # Sync to Keycloak project group AND the main-cloud LibreGraph group.
        # Both writes are load-bearing — see sync_project_member_to_groups.
        user = await store.get_user(body.user_id)
        if user:
            await project_provisioning.sync_project_member_to_groups(
                project_id,
                f"project-{project_id}",
                user,
                backend,
                dependencies=dependencies.provisioning,
            )

        # Grant Gitea access to all managed project repos
        if dependencies.forge.is_initialized:
            try:
                if user and user.get("email"):
                    repos = await store.get_project_repositories(project_id)
                    for repo in repos:
                        if repo.get("is_managed"):
                            await dependencies.forge.grant_user_repo_access(
                                user["email"], repo["name"]
                            )
            except Exception as e:
                logger.warning(
                    f"Failed to grant Gitea access for member {body.user_id}: {e}"
                )

        return result
    except Exception as e:
        if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail="User is already a member")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def update_project_member(
    project_id: str,
    user_id: str,
    body: ProjectMemberUpdate,
    *,
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Update a member's role in a project. Caller must be a project owner or admin."""
    success = await dependencies.store.update_project_member_role(
        project_id=project_id, user_id=user_id, role=body.role
    )
    if not success:
        raise HTTPException(status_code=404, detail="Member not found")
    return {"status": "updated"}


async def remove_project_member(
    project_id: str,
    user_id: str,
    *,
    caller: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Remove a member from a project. Owner/admin can remove anyone; any member can remove themselves. Cannot remove the last owner."""
    store = dependencies.store
    logger = dependencies.logger

    # H3: pre-fix, anyone could remove anyone (only the last-owner check
    # was enforced). Allow self-removal so members can leave projects.
    if str(caller["id"]) != str(user_id) and not caller.get("is_admin"):
        caller_role = await store.get_user_role_in_project(
            project_id, str(caller["id"])
        )
        if caller_role != "owner":
            raise HTTPException(status_code=403, detail="Project owner role required")

    # Check if this is the last owner
    role = await store.get_user_role_in_project(project_id, user_id)
    if role == "owner":
        members = await store.get_project_members(project_id)
        owner_count = sum(1 for m in members if m.get("role") == "owner")
        if owner_count <= 1:
            raise HTTPException(
                status_code=400, detail="Cannot remove the last owner of a project"
            )

    success = await store.remove_project_member(project_id, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Member not found")

    # Remove from Keycloak group
    if dependencies.keycloak_groups.is_initialized:
        user = await store.get_user(user_id)
        if user and user.get("keycloak_sub"):
            await dependencies.keycloak_groups.remove_user_from_project_group(
                user["keycloak_sub"], project_id
            )

    # Revoke Gitea access from managed project repos
    if dependencies.forge.is_initialized:
        try:
            removed_user = await store.get_user(user_id)
            if removed_user and removed_user.get("email"):
                repos = await store.get_project_repositories(project_id)
                for repo in repos:
                    if repo.get("is_managed"):
                        await dependencies.forge.revoke_user_repo_access(
                            removed_user["email"], repo["name"]
                        )
        except Exception as e:
            logger.warning(f"Failed to revoke Gitea access for member {user_id}: {e}")

    # Main-cloud group removal flows through Keycloak — OpenCloud reconciles
    # LibreGraph memberships from the OIDC `groups` claim on login, so the
    # Keycloak-side remove above is sufficient. No direct backend.remove call
    # needed.

    return {"status": "removed"}


# =============================================================================
# Project repositories
# =============================================================================


async def list_project_repositories(
    project_id: str,
    *,
    role: str | None,
    dependencies: ProjectDependencies,
) -> list[dict[str, Any]]:
    """List repositories attached to a project."""
    rows = await dependencies.store.get_project_repositories(project_id, role=role)
    # A/C: strip embedded Gitea credentials and rewrite the internal host to the
    # ingress-routable one before the row leaves the orchestrator (see
    # redact_repositories / externalize_gitea_url).
    return redact_repositories(rows)


async def add_project_repository(
    project_id: str,
    body: ProjectRepositoryCreate,
    *,
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Attach a repository to a project. Owner or admin only (creates managed Gitea repo)."""
    store = dependencies.store
    logger = dependencies.logger

    repo_url = body.repo_url
    is_managed = False
    repository_id: str | None = None
    creation_intent: dict[str, Any] | None = None

    # Create a managed Gitea repo if requested
    if body.create_managed and dependencies.forge.is_initialized:
        repository_id = str(uuid4())
        access_mode = (
            "none"
            if body.role == "knowledge"
            else "read"
            if body.role == "reference" or body.read_only
            else "write"
        )
        try:
            repo_url, creation_intent = await create_managed_repository(
                store,
                dependencies.forge,
                repo_name=body.name,
                authority_kind="project_repository",
                authority_id=repository_id,
                project_id=project_id,
                access_mode=access_mode,
            )
        except ManagedRepositoryAuthorityError as exc:
            raise HTTPException(
                status_code=502, detail="Failed to create Gitea repository"
            ) from exc
        is_managed = True

    created: dict[str, Any] | None = None
    try:
        created = await store.add_project_repository(
            project_id=project_id,
            repository_id=repository_id,
            name=body.name,
            repo_url=repo_url,
            role=body.role,
            description=body.description,
            read_only=body.read_only,
            is_managed=is_managed,
            branch=body.branch,
            clone_path=body.clone_path,
        )
        if is_managed and project_repository_access_mode(created) is not None:
            assert creation_intent is not None
            await ensure_project_repository_authority(
                store,
                dependencies.forge,
                created,
                creation_intent_id=str(creation_intent["id"]),
            )
        # Keep the historical redaction boundary even though new managed rows
        # are credential-free. It remains the safe projection for old data.
        return redact_repository(created)
    except Exception as exc:
        # If we created a managed repo but DB insert failed, clean up
        if is_managed and repo_url:
            contained = await revoke_and_delete_managed_repository(
                store, dependencies.forge, body.name
            )
            if contained and created is not None:
                await store.remove_project_repository(str(created["id"]))
        logger.warning(
            "Managed project repository creation failed for project %s",
            project_id,
        )
        raise HTTPException(
            status_code=500,
            detail="Managed repository provisioning failed",
        ) from exc


async def update_project_repository(
    project_id: str,
    repo_id: str,
    body: ProjectRepositoryUpdate,
    *,
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Update a project repository. Owner or admin only."""
    store = dependencies.store

    kwargs = {k: v for k, v in body.model_dump().items() if v is not None}
    if not kwargs:
        raise HTTPException(status_code=400, detail="No fields to update")
    repository = await store.get_project_repository(repo_id)
    if repository is None or str(repository.get("project_id")) != project_id:
        raise HTTPException(status_code=404, detail="Repository not found")
    if (
        repository.get("is_managed")
        and "name" in kwargs
        and str(kwargs["name"]) != str(repository.get("name"))
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "managed_repository_identity_immutable",
                "message": "Create a new managed repository instead of renaming it",
            },
        )
    if (
        repository.get("is_managed")
        and "read_only" in kwargs
        and bool(kwargs["read_only"]) != bool(repository.get("read_only"))
    ):
        target_repository = {**repository, **kwargs}
        try:
            await rotate_project_repository_authority(
                store,
                dependencies.forge,
                target_repository,
            )
        except ManagedRepositoryAuthorityError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "managed_repository_authority_rotation_failed",
                    "message": "Repository access-mode rotation is retryable",
                },
            ) from exc
    success = await store.update_project_repository(repo_id, **kwargs)
    if not success:
        raise HTTPException(status_code=404, detail="Repository not found")
    return {"status": "updated"}


async def remove_project_repository(
    project_id: str,
    repo_id: str,
    *,
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Remove a repository from a project. Owner or admin only. Cannot remove the jobs repo."""
    store = dependencies.store

    repo = await store.get_project_repository(repo_id)
    if not repo or str(repo.get("project_id")) != project_id:
        raise HTTPException(status_code=404, detail="Repository not found")
    if repo.get("role") == "jobs":
        raise HTTPException(status_code=400, detail="Cannot remove the jobs repository")

    # A managed row is the durable retry handle for key revocation. Contain
    # its credential before deleting that row; otherwise a transient Gitea
    # outage would turn it into an unowned, still-usable workspace bearer.
    if repo.get("is_managed"):
        if not await revoke_and_delete_managed_repository(
            store, dependencies.forge, repo["name"]
        ):
            raise HTTPException(
                status_code=503,
                detail="Repository credential revocation is retryable",
            )

    removed = await store.remove_project_repository(repo_id)
    if not removed:
        raise HTTPException(status_code=500, detail="Failed to remove repository")

    return {"status": "removed"}


# =============================================================================
# Project Datasources (N:M)
# =============================================================================


async def list_project_linkable_datasources(
    project_id: str,
    *,
    user: dict[str, Any],
    q: str | None,
    limit: int,
    cursor: str | None,
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Page connectors the caller may newly link to the target project."""
    try:
        result = await dependencies.store.list_project_linkable_datasources(
            str(user["id"]),
            project_id,
            is_admin=bool(user.get("is_admin")),
            q=q,
            limit=limit,
            cursor=cursor,
        )
    except DatasourceCatalogCursorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result["items"] = redact_datasources(result.get("items") or [])
    return result


async def list_project_datasources(
    project_id: str, *, dependencies: ProjectDependencies
) -> list[dict[str, Any]]:
    """List connectors linked to a project. F3: project membership required."""
    try:
        rows = await dependencies.store.list_project_datasources(project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return redact_datasources(rows)


async def link_datasource_to_project(
    project_id: str,
    datasource_id: str,
    body: ProjectDatasourceSettings | None,
    *,
    user: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Link an existing connector to a project.

    F3: caller must be project owner of the target project AND must be
    able to see the connector (admin / creator / member of one of its
    projects). Prevents a project owner from probing for stranger
    connectors by guessing UUIDs.

    Optionally pass project-level overrides (read_only, description).
    Also creates a knowledge entry so agents discover the connector.
    """
    store = dependencies.store

    ds = await store.get_datasource(datasource_id)
    if not ds:
        raise HTTPException(
            status_code=404, detail=f"Connector '{datasource_id}' not found"
        )
    if not (ds.get("is_global") or await user_can_access_datasource(user, store, ds)):
        raise HTTPException(
            status_code=403, detail="Not authorized to link this connector"
        )

    from orchestrator.services.kb_datasources import native_kb_project_id

    if native_kb_project_id(ds):
        raise HTTPException(
            status_code=409,
            detail="The native knowledge connector cannot be linked to another project",
        )
    is_connector_owner = str(ds.get("created_by") or "") == str(user["id"])
    if (ds.get("scope_mode") == "projects" or not ds.get("is_global")) and not (
        user.get("is_admin") or is_connector_owner
    ):
        raise HTTPException(
            status_code=403,
            detail="Only the connector owner or an admin may add this project link",
        )

    try:
        effective_read_only = (
            True if ds.get("type") == "kb" else (body.read_only if body else None)
        )
        await store.link_datasource_to_project(
            project_id,
            datasource_id,
            read_only=effective_read_only,
            description=body.description if body else None,
            authority_user_id=str(user["id"]),
            authority_is_admin=bool(user.get("is_admin")),
        )
        return {"status": "linked"}
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to add one or more project links",
        ) from exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def update_project_datasource(
    project_id: str,
    datasource_id: str,
    body: ProjectDatasourceSettings,
    *,
    user: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Update project-level settings for a linked connector. F3: project owner only.

    Pass null to clear an override and fall back to connector defaults.
    """
    store = dependencies.store

    ds = await store.get_datasource(datasource_id)
    if not ds:
        raise HTTPException(
            status_code=404, detail=f"Connector '{datasource_id}' not found"
        )
    effective_read_only = True if ds.get("type") == "kb" else body.read_only
    try:
        success = await store.update_project_datasource(
            project_id,
            datasource_id,
            read_only=effective_read_only,
            description=body.description,
            authority_user_id=str(user["id"]),
            authority_is_admin=bool(user.get("is_admin")),
        )
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to modify this project connector link",
        ) from exc
    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Link between project '{project_id}' and connector '{datasource_id}' not found",
        )

    return {"status": "updated"}


async def resolve_datasource_unlink(
    project_id: str,
    datasource_id: str,
    *,
    user: dict[str, Any],
    dependencies: ProjectDependencies,
) -> tuple[dict[str, Any], bool]:
    """Everything the unlink path decides before the project-owner escalation.

    Returns the connector row and whether the caller still has to satisfy
    ``require_project_owner``. Split out of :func:`unlink_datasource_from_project`
    only so the router can run that gate itself: the checks and their order are
    unchanged, and the escalation stays exactly where it was — after the MCP
    scope check, the connector lookup and the native-KB refusal.
    """
    scope_project_id = mcp_scope_project_id(user)
    if scope_project_id and str(scope_project_id) != str(project_id):
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        )
    ds = await dependencies.store.get_datasource(datasource_id)
    if not ds:
        raise HTTPException(status_code=404, detail="Connector not found")
    from orchestrator.services.kb_datasources import native_kb_project_id

    if native_kb_project_id(ds):
        raise HTTPException(
            status_code=409,
            detail="The native knowledge connector link is managed by its project",
        )
    is_connector_owner = str(ds.get("created_by") or "") == str(user["id"])
    return ds, not (user.get("is_admin") or is_connector_owner)


async def unlink_datasource_from_project(
    project_id: str,
    datasource_id: str,
    *,
    user: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, str]:
    """Unlink a connector from a project. F3: project owner only.

    Also removes the knowledge entry.
    """
    try:
        removed = await dependencies.store.unlink_datasource_from_project(
            project_id,
            datasource_id,
            authority_user_id=str(user["id"]),
            authority_is_admin=bool(user.get("is_admin")),
        )
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to modify this project connector link",
        ) from exc
    if not removed:
        raise HTTPException(
            status_code=404,
            detail=f"Link between project '{project_id}' and connector '{datasource_id}' not found",
        )

    return {"status": "unlinked"}


# =============================================================================
# Project job records and promotion
# =============================================================================


async def list_project_job_records(
    project_id: str, *, limit: int, dependencies: ProjectDependencies
) -> list[dict[str, Any]]:
    """Return orchestrator-owned terminal history for a project."""
    return await dependencies.store.list_project_job_change_records(
        project_id, limit=limit
    )


async def get_job_change_record(
    job_id: str, *, dependencies: ProjectDependencies
) -> dict[str, Any]:
    """Return the immutable structured terminal record for one visible job."""
    record = await dependencies.store.get_job_change_record(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job change record not found")
    return record


async def promote_job(
    job_id: str,
    body: PromoteRequest,
    *,
    caller: dict[str, Any],
    dependencies: ProjectDependencies,
) -> dict[str, Any]:
    """Promote a default-project job into a dedicated project.

    Creates a new project, provisions its cloud/knowledge resources, and moves
    the completed job into it. The job keeps its isolated execution repository;
    no project workspace repository is created.

    P4c: ``body.user_id`` is forced to the caller (mirrors F2 — no cross-user
    promotion).
    """
    store = dependencies.store
    logger = dependencies.logger

    body.user_id = caller["id"]

    try:
        # Validate job
        job = await store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        if job["status"] != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Job must be completed to promote (status: {job['status']})",
            )

        # Verify job is in a default project
        old_project_id = str(job["project_id"]) if job.get("project_id") else None
        if not old_project_id:
            raise HTTPException(
                status_code=400,
                detail="Job has no project_id — cannot determine source project",
            )

        old_project = await store.get_project(old_project_id)
        if not old_project:
            raise HTTPException(
                status_code=400,
                detail=f"Source project '{old_project_id}' not found",
            )

        if not old_project.get("is_default"):
            raise HTTPException(
                status_code=400,
                detail="Job can only be promoted from a default project",
            )

        # Create new project
        new_project = await store.create_project(
            name=body.name,
            description=body.description,
            goal=body.goal,
        )
        new_project_id = str(new_project["id"])

        # Add user as owner
        await store.add_project_member(
            project_id=new_project_id,
            user_id=body.user_id,
            role="owner",
        )

        # Provision the project-owned resources. The promoted job's own repo is
        # retained as its historical execution surface; it is never promoted
        # into an implicit template for future jobs.
        if dependencies.forge.is_initialized:
            try:
                await project_provisioning.provision_project_knowledge_repo(
                    new_project, body.user_id, dependencies=dependencies.provisioning
                )
            except Exception as e:
                logger.warning(
                    f"Promote: knowledge provisioning failed for {new_project_id}: {e}"
                )
        new_project = await project_provisioning.ensure_project_cloud_resources(
            new_project, dependencies=dependencies.provisioning
        )

        # Move job to new project
        async with store.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET project_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                UUID(new_project_id),
                UUID(job_id),
            )

        logger.info(
            f"Promoted job {job_id[:8]} to project '{body.name}' ({new_project_id[:8]})"
        )

        return {
            "status": "promoted",
            "project_id": new_project_id,
            "project_name": body.name,
            "job_id": job_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Promote failed for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e
