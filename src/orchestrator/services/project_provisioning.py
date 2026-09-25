"""Provisioning and drift repair for a project's cloud, group and vault state.

Everything here is *optional* tier work hung off the two primary write paths
(``create_project`` and the project read page): a Keycloak group, a main-cloud
Space, the knowledge repository, and the ``kb`` connector that fronts it. None
of it may fail a project that has already been written — which is why every
remote effect below logs and degrades rather than raising, and why the one
place that still refuses (``MainCloudRouter.for_owner``) is caught by its
caller instead of being weakened.

The mutable pieces — the per-project heal locks and the background-repair
bookkeeping — live on :class:`ProjectRepairState`, one instance per
application, rather than as module globals a second app instance would share.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from fastapi import HTTPException

from orchestrator.schemas.projects import ExternalKnowledgeBase
from orchestrator.services.cloud import FeatureNotAvailable
from orchestrator.services.cloud.identity import resolve_user_identity_cached
from orchestrator.services.datasource_config import (
    validate_kb_repository_auth,
    validate_kb_repository_url,
)
from orchestrator.services.knowledge_index import (
    KnowledgeIndexDependencies,
    purge_kb_datasource_index,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    create_managed_repository,
)

BG_REPAIR_COOLDOWN_S = 3600.0


@dataclass
class ProjectRepairState:
    """The mutable bookkeeping the repair paths need, owned by one app.

    Per-project locks prevent concurrent heal attempts from creating duplicate
    Spaces (ensure_project_folder is not idempotent — each call makes a new
    drive).

    Fire-and-forget repair tasks spawned off the project-GET read path (see
    knowledge-base/knowledge/issues/project_page_open_blocks_on_cloud_heal.md part 1). The task set
    holds strong references (bare create_task results are GC-able); the
    last-fired map throttles per key so page views don't hammer Keycloak/the
    cloud backend. Both are bounded by the number of live projects/users.
    """

    heal_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    bg_repair_tasks: set[asyncio.Task] = field(default_factory=set)
    bg_repair_last: dict[str, float] = field(default_factory=dict)
    bg_repair_cooldown_s: float = BG_REPAIR_COOLDOWN_S

    async def drain(self) -> None:
        """Cancel and await every in-flight background repair (R1.B12).

        The application calls this at shutdown, before its stores close. A
        repair is re-derived from drift on a later project read, so a cancelled
        one leaves nothing to hand over. The task set is left empty and usable;
        the heal locks and the per-key throttle are bookkeeping, not tasks, and
        are left as they are. The calling task is never cancelled.
        """
        current = asyncio.current_task()
        drained = [task for task in self.bg_repair_tasks if task is not current]
        pending = [task for task in drained if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in drained:
            self.bg_repair_tasks.discard(task)


@dataclass(frozen=True)
class ProjectProvisioningDependencies:
    """Per-invocation collaborators for project provisioning and repair.

    ``store``, ``forge``, ``keycloak_groups`` and ``main_cloud_router`` are all
    rebound during the application's ``lifespan``; they arrive here as values
    resolved by the factory that built this dataclass, never captured at
    import.
    """

    store: Any
    forge: Any
    keycloak_groups: Any
    main_cloud_router: Any
    logger: Any
    repair: ProjectRepairState
    knowledge_index: KnowledgeIndexDependencies


def fire_background_repair(
    key: str,
    coro: Coroutine[Any, Any, Any],
    *,
    dependencies: ProjectProvisioningDependencies,
) -> bool:
    """Run ``coro`` as a fire-and-forget task, at most once per cooldown per key.

    Returns True if the task was scheduled, False if throttled (the coroutine
    is closed so it never warns "never awaited").
    """
    repair = dependencies.repair
    now = time.monotonic()
    last = repair.bg_repair_last.get(key)
    if last is not None and now - last < repair.bg_repair_cooldown_s:
        coro.close()
        return False
    repair.bg_repair_last[key] = now
    task = asyncio.create_task(coro)
    repair.bg_repair_tasks.add(task)
    task.add_done_callback(repair.bg_repair_tasks.discard)
    return True


async def sync_project_member_to_groups(
    project_id_str: str,
    group_name: str,
    user: dict[str, Any],
    backend: Any,
    *,
    dependencies: ProjectProvisioningDependencies,
) -> None:
    """Add one member to the Keycloak group and the LibreGraph group.

    Both writes are needed: Keycloak carries the `groups` token claim (durable
    across OpenCloud re-logins), the direct backend add makes the Space visible
    in the user's currently-active OpenCloud session without waiting for them
    to re-authenticate.
    """
    if dependencies.keycloak_groups.is_initialized and user.get("keycloak_sub"):
        await dependencies.keycloak_groups.add_user_to_project_group(
            user["keycloak_sub"], project_id_str
        )
    if backend.is_initialized:
        try:
            resolved = await resolve_user_identity_cached(
                dependencies.store, user, backend
            )
            if resolved:
                await backend.add_user_to_group(resolved, group_name)
        except Exception as e:
            dependencies.logger.debug(
                f"Failed to add user {user.get('id')} to backend group "
                f"{group_name}: {e}"
            )


async def ensure_project_cloud_resources(
    project: dict[str, Any],
    *,
    dependencies: ProjectProvisioningDependencies,
) -> dict[str, Any]:
    """Ensure a project has its Keycloak group, main-cloud Space and WebDAV
    datasource — and that every current member is in both groups.

    Two conditional steps:

    * **Folder creation** runs only when `main_cloud_folder_handle` is missing
      (protected by a per-project lock because `ensure_project_folder` is not
      idempotent — each call creates a new Space).
    * **Member sync** runs unconditionally (the Keycloak and LibreGraph adds
      are idempotent). This repairs the common case where the Space already
      exists but the user was never added to the groups — e.g. project created
      while Keycloak admin auth was broken, or a Space adopted out-of-band.
      Primary membership writes happen at mutation time (add_project_member);
      this is pure drift repair, which is why get_project runs it as a
      throttled background task rather than on the request path (it costs
      several external HTTP round-trips — see
      knowledge-base/knowledge/issues/project_page_open_blocks_on_cloud_heal.md).

    Default projects skip both: they piggyback on the owner's personal home
    Space (already attached as a datasource by the user-creation flow), so a
    separate project Space + `project-<id>` group would be dead state. The
    cloud_storage_url branch in `get_project` resolves to the user's home
    browser URL for default projects.

    Returns the (possibly updated) project dict.
    """
    if project.get("is_default"):
        return project

    store = dependencies.store
    logger = dependencies.logger
    keycloak_groups = dependencies.keycloak_groups

    project_id_str = str(project["id"])
    project_name = project["name"]
    group_name = f"project-{project_id_str}"
    if project.get("main_cloud_backend"):
        backend = dependencies.main_cloud_router.for_project_optional(project)
        if backend is None:
            # Every step below is a remote effect (ensure_group,
            # ensure_project_folder, LibreGraph member adds) and this row's
            # installation authority is unresolvable — a pre-0186 row, or an
            # instance this replica has not cached. Guessing an installation is
            # exactly what we must not do, and this helper is drift repair, not
            # a primary write path (add_project_member owns those). So skip,
            # loudly. Stamping the row (backfill) is what re-enables it.
            logger.warning(
                "Project %s: skipping cloud resource heal — no resolvable "
                "backend instance for provider %r (row needs a "
                "main_cloud_backend_instance_id backfill).",
                project_id_str,
                project.get("main_cloud_backend"),
            )
            return project
    else:
        try:
            backend = dependencies.main_cloud_router.for_owner()
        except FeatureNotAvailable as e:
            # No active backend instance is bound — an installation with no
            # main cloud configured at all. `for_owner` is right to refuse
            # rather than guess an installation, but this helper is optional
            # provisioning, not a primary write: `create_project` has already
            # committed the project and its owner membership by the time it
            # gets here, and the only caller that treats a raise as fatal is
            # its blanket 500. Degrade exactly as the `for_project_optional`
            # branch above does, and as every remote effect below already
            # does. A cloudless installation gets a project without cloud
            # resources, which is the whole point of the optional tier.
            logger.info(
                "Project %s: skipping cloud resource provisioning — no active "
                "main-cloud backend instance (%s).",
                project_id_str,
                e,
            )
            return project

    handle_str = project.get("main_cloud_folder_handle")
    legacy_folder_id = project.get("nextcloud_folder_id")
    needs_folder = not (handle_str or legacy_folder_id)

    if needs_folder and backend.is_initialized:
        lock = dependencies.repair.heal_locks.setdefault(project_id_str, asyncio.Lock())
        async with lock:
            fresh = await store.get_project(project_id_str)
            if fresh:
                project = fresh
            if not (
                project.get("main_cloud_folder_handle")
                or project.get("nextcloud_folder_id")
            ):
                try:
                    if keycloak_groups.is_initialized:
                        await keycloak_groups.ensure_project_group(
                            project_id_str, project_name
                        )
                    await backend.ensure_group(group_name)
                    folder_handle = await backend.ensure_project_folder(
                        project_name=project_name,
                        group_id=group_name,
                    )
                    legacy_id: int | None = None
                    if backend.backend_id == "nextcloud":
                        try:
                            legacy_id = int(folder_handle.native_id)
                        except ValueError:
                            legacy_id = None
                    await store.update_project(
                        project_id_str,
                        main_cloud_backend=backend.backend_id,
                        main_cloud_backend_instance_id=(backend.backend_instance_id),
                        main_cloud_folder_handle=folder_handle.to_db(),
                        nextcloud_folder_id=legacy_id,
                    )
                    project["main_cloud_backend"] = backend.backend_id
                    project["main_cloud_backend_instance_id"] = (
                        backend.backend_instance_id
                    )
                    project["main_cloud_folder_handle"] = folder_handle.to_db()
                    project["nextcloud_folder_id"] = legacy_id

                    # The project working folder is intentionally NOT attached
                    # as a `webdav` datasource: job/session workspaces get the
                    # folder cloned in (Mode-A baseline for jobs, the `projects/`
                    # sync mount for sessions), so attaching it here would
                    # double-expose the same files through the webdav_* tools.
                    # webdav_* tools are reserved for clouds that are NOT cloned
                    # (the personal home cloud + externally-attached WebDAV).
                    # See knowledge-base/knowledge/issues/main_cloud.md (Issue 1 / Issue 8).
                except Exception as e:
                    logger.warning(
                        f"Failed to create cloud resources for project "
                        f"{project_id_str}: {e}"
                    )

    # Member sync — always runs when this helper does. Idempotent add in both
    # Keycloak and the backend; essential for self-healing projects whose
    # Space exists but whose members were never added to the groups. Callers
    # keep it off latency-sensitive paths (fire_background_repair).
    if keycloak_groups.is_initialized or backend.is_initialized:
        try:
            if keycloak_groups.is_initialized:
                await keycloak_groups.ensure_project_group(project_id_str, project_name)
            members = await store.get_project_members(project_id_str)
            if members:
                users = await asyncio.gather(
                    *[store.get_user(str(m["user_id"])) for m in members],
                    return_exceptions=False,
                )
                await asyncio.gather(
                    *[
                        sync_project_member_to_groups(
                            project_id_str,
                            group_name,
                            u,
                            backend,
                            dependencies=dependencies,
                        )
                        for u in users
                        if u
                    ],
                    return_exceptions=True,
                )
        except Exception as e:
            logger.debug(f"Member sync failed for project {project_id_str}: {e}")

    return project


async def provision_project_knowledge_repo(
    project: dict[str, Any],
    owner_id: str | None,
    *,
    dependencies: ProjectProvisioningDependencies,
) -> dict[str, Any] | None:
    """Create the project's knowledge repo and attach it as its own KB.

    Two artifacts, in this order:

    * ``project-<id8>-knowledge`` registered with role ``knowledge`` — the
      vault ``resolve_kb_repo`` prefers from now on, and the repo the
      materialisation endpoint commits notes into.
    * a ``kb`` datasource linked to the project — the design's
      "default-attached datasource" (§6). It is a **management surface**:
      something to see, list and unlink in the cockpit. It is *not* an index
      of its own. Its notes are indexed under ``kb_id = project_id`` by the
      native sweep, which is why the row carries the ``native_project_id``
      marker: without it the external sweep would index the same vault a
      second time under this row's UUID and every note would come back twice
      in search (§8, criterion 5).

    ``connection_url`` is deliberately left null. Managed URLs are now
    credential-free, but the connector is not repository transport authority;
    a second copy of "where the vault lives" is exactly the divergence §10
    warns about. ``project_repositories`` remains the one authoritative answer.

    Returns the created datasource row, or None when Gitea refused the repo.
    """
    from orchestrator.services.kb_datasources import NATIVE_PROJECT_CONFIG_KEY

    store = dependencies.store
    logger = dependencies.logger

    project_id = str(project["id"])
    id8 = project_id[:8]
    repo_name = f"project-{id8}-knowledge"
    repository_id = str(uuid4())

    try:
        repo_url, _creation_intent = await create_managed_repository(
            store,
            dependencies.forge,
            repo_name=repo_name,
            authority_kind="project_repository",
            authority_id=repository_id,
            project_id=project_id,
            access_mode="none",
        )
    except ManagedRepositoryAuthorityError:
        logger.warning(
            "Gitea did not create knowledge repo '%s'; project %s has no "
            "file-backed vault until provisioning is retried",
            repo_name,
            project_id,
        )
        return None

    await store.add_project_repository(
        project_id=project_id,
        repository_id=repository_id,
        name=repo_name,
        repo_url=repo_url,
        role="knowledge",
        description="Project knowledge vault (OKF notes under knowledge/)",
        is_managed=True,
    )
    if owner_id:
        try:
            creator = await store.get_user(owner_id)
            if creator and creator.get("email"):
                await dependencies.forge.grant_user_repo_access(
                    creator["email"], repo_name
                )
        except Exception as e:
            logger.warning(
                f"Failed to grant Gitea access for project creator on "
                f"knowledge repo: {e}"
            )

    # Names are unique per (name, type, owner), so the id8 keeps two projects
    # with the same title from colliding on their KB connectors.
    datasource = await store.create_datasource(
        name=f"{project['name']} Knowledge ({id8})",
        ds_type="kb",
        connection_url=None,
        description=(
            f"This project's knowledge base, stored in `{repo_name}`. "
            "Notes written with kb_write land here."
        ),
        config={"root_path": "knowledge", NATIVE_PROJECT_CONFIG_KEY: project_id},
        created_by=owner_id,
        read_only=True,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project_id],
    )
    logger.info(
        "Provisioned knowledge repo '%s' + KB connector %s for project %s",
        repo_name,
        datasource["id"],
        project_id,
    )
    return datasource


@dataclass(frozen=True, repr=False)
class KbVaultPlan:
    """A validated live-vault target, ready to provision.

    It carries the PAT, so it stays local to the provisioning call: never
    logged, never returned (hence the suppressed repr). ``datasource_id`` set
    means an existing connector is being adopted in place instead of a new row
    being created for the same repository.
    """

    repo_url: str
    branch: str
    forge: str
    owner: str
    repo: str
    credentials: dict[str, str]
    explicit_forge: bool
    datasource_id: str | None = None
    policy_revision: int = 0


def kb_vault_plan(
    repo_url: str | None,
    branch: str,
    credentials: dict[str, str],
    forge_override: str | None,
    *,
    datasource_id: str | None = None,
    policy_revision: int = 0,
) -> KbVaultPlan:
    """Validate a writable-vault target's URL, transport auth and forge."""
    from shared.runtime.services.forge import ForgeError, parse_owner_repo

    validated_url = validate_kb_repository_url(repo_url)
    validate_kb_repository_auth(validated_url, credentials)

    host = (urlparse(validated_url).hostname or "").lower().rstrip(".")
    explicit = forge_override is not None
    forge = forge_override or (
        "github" if host in {"github.com", "www.github.com"} else ""
    )
    if forge != "github":
        raise HTTPException(
            status_code=400,
            detail=(
                "External writable project KBs currently support GitHub only; "
                "set forge='github' for GitHub Enterprise"
            ),
        )
    try:
        owner, repo = parse_owner_repo(validated_url)
    except ForgeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return KbVaultPlan(
        repo_url=validated_url,
        branch=branch,
        forge=forge,
        owner=owner,
        repo=repo,
        credentials=credentials,
        explicit_forge=explicit,
        datasource_id=datasource_id,
        policy_revision=policy_revision,
    )


async def plan_kb_vault_from_connector(
    datasource_id: str,
    caller: dict[str, Any] | None,
    project_id: str | None,
    *,
    dependencies: ProjectProvisioningDependencies,
) -> KbVaultPlan:
    """Check that an existing ``kb`` connector can become a project's vault.

    Adoption converts the row (see ``adopt_kb_connector_as_vault``), so every
    reason it must not be converted is checked here, before anything is
    created.
    """
    from orchestrator.services.kb_datasources import native_kb_project_id

    store = dependencies.store

    datasource = await store.get_datasource(datasource_id)
    if not datasource:
        raise HTTPException(
            status_code=404, detail=f"Connector '{datasource_id}' not found"
        )
    if str(datasource.get("type") or "") != "kb":
        raise HTTPException(
            status_code=400,
            detail=(
                "Only an OKF Knowledge Base connector can back a project knowledge base"
            ),
        )
    is_owner = str(datasource.get("created_by") or "") == str(
        (caller or {}).get("id") or ""
    )
    if not ((caller or {}).get("is_admin") or is_owner):
        raise HTTPException(
            status_code=403,
            detail="Not authorized to use this connector as a knowledge base",
        )
    if native_kb_project_id(datasource):
        raise HTTPException(
            status_code=409,
            detail="This connector is already a project's knowledge base",
        )
    if datasource.get("is_global"):
        # Adoption takes the row private and drops the index every other user
        # was reading. Same reasoning as the project-link check below.
        raise HTTPException(
            status_code=409,
            detail=(
                "This connector is published to everyone; unpublish it before "
                "using it as a project knowledge base"
            ),
        )
    linked = await store.list_datasource_projects(datasource_id)
    others = sorted(
        {str(value) for value in (linked or [])}
        - ({str(project_id)} if project_id else set())
    )
    if others:
        # Adoption narrows the connector to exactly one project. Doing that
        # silently would revoke every other project's reader access — and
        # their agents would keep listing a KB whose index no longer exists.
        raise HTTPException(
            status_code=409,
            detail=(
                "This connector is shared with other projects; unlink it from "
                "them before using it as a project knowledge base"
            ),
        )

    config = datasource.get("config") or {}
    if not isinstance(config, dict):
        config = {}
    root_path = str(config.get("root_path") or "").strip().strip("/")
    if root_path not in ("", "knowledge"):
        # kb_materialize commits every note to ``knowledge/<slug>.md``; any
        # other root would read a different folder than the agent writes.
        raise HTTPException(
            status_code=400,
            detail=(
                f"A project knowledge base is written to 'knowledge/', but this "
                f"connector reads '{root_path}/'. Set its note root to "
                "'knowledge' first."
            ),
        )

    credentials = datasource.get("credentials") or {}
    if isinstance(credentials, str):
        try:
            credentials = json.loads(credentials)
        except (json.JSONDecodeError, ValueError):
            credentials = {}
    token = (
        str(credentials.get("token") or "").strip()
        if isinstance(credentials, dict)
        else ""
    )
    if not token:
        # Reads clone over git; writes go through the GitHub contents API,
        # which has no SSH equivalent. An SSH-only connector would index fine
        # and fail on every single note write.
        raise HTTPException(
            status_code=400,
            detail=(
                "A writable project knowledge base needs a token credential; "
                "this connector has none (an SSH key cannot write notes)"
            ),
        )

    return kb_vault_plan(
        datasource.get("connection_url"),
        str(datasource.get("default_branch") or "main"),
        {"auth_method": "token", "token": token},
        str(config.get("forge") or "") or None,
        datasource_id=str(datasource["id"]),
        policy_revision=int(datasource.get("policy_revision") or 0),
    )


async def plan_external_kb_vault(
    external_kb: ExternalKnowledgeBase,
    *,
    caller: dict[str, Any] | None = None,
    project_id: str | None = None,
    dependencies: ProjectProvisioningDependencies,
) -> KbVaultPlan:
    """Resolve either request mode into one validated plan.

    Callers run this *before* creating anything, so a rejected vault cannot
    leave a half-created project behind.
    """
    if external_kb.datasource_id:
        return await plan_kb_vault_from_connector(
            external_kb.datasource_id,
            caller,
            project_id,
            dependencies=dependencies,
        )
    return kb_vault_plan(
        external_kb.repo_url,
        external_kb.branch,
        {
            "auth_method": "token",
            "token": (
                external_kb.token.get_secret_value().strip()
                if external_kb.token
                else ""
            ),
        },
        external_kb.forge,
    )


async def adopt_kb_connector_as_vault(
    plan: KbVaultPlan,
    project_id: str,
    config: dict[str, Any],
    *,
    dependencies: ProjectProvisioningDependencies,
) -> dict[str, Any]:
    """Convert an existing connector into this project's vault row, in place.

    Copying it into a second row would leave two connectors on one repository:
    the copy indexed under the project id and the original still swept under
    its own UUID, so every note would answer a search twice — the one failure
    in knowledge-base/knowledge/features/knowledge_base_repo_separation.md that corrupts search
    rather than merely failing it.

    Scope narrows to the adopting project because the row stops being an
    external source the moment the marker lands: anyone else still holding a
    link would bind a KB whose index no longer exists.
    """
    updated = await dependencies.store.update_datasource_with_policy(
        str(plan.datasource_id),
        expected_policy_revision=plan.policy_revision,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[project_id],
        config=config,
    )
    if updated is None:
        raise RuntimeError("Knowledge connector disappeared during adoption")
    return updated


async def provision_external_project_knowledge_repo(
    project: dict[str, Any],
    owner_id: str | None,
    plan: KbVaultPlan,
    *,
    dependencies: ProjectProvisioningDependencies,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Attach an existing GitHub repo and store its PAT on the native KB row."""
    from orchestrator.services.kb_datasources import NATIVE_PROJECT_CONFIG_KEY

    store = dependencies.store
    logger = dependencies.logger

    repo_url = plan.repo_url
    branch = plan.branch
    forge = plan.forge
    repo_name = plan.repo
    credentials = plan.credentials
    project_id = str(project["id"])
    id8 = project_id[:8]
    datasource_ref = await store.get_native_project_kb_datasource_ref(project_id)
    if (
        plan.datasource_id
        and datasource_ref
        and str(datasource_ref.get("id") or "") != str(plan.datasource_id)
    ):
        # Two rows marked native to one project would make the credential
        # lookup (oldest wins) resolve to a different repository than the one
        # the vault reads and writes.
        raise HTTPException(
            status_code=409,
            detail=(
                "This project already has a knowledge connector; remove it "
                "before attaching another"
            ),
        )
    repo_row = await store.add_project_repository(
        project_id=project_id,
        name=repo_name,
        repo_url=repo_url,
        role="knowledge",
        description="External project knowledge vault (OKF notes under knowledge/)",
        is_managed=False,
        branch=branch,
    )
    config = {"root_path": "knowledge", NATIVE_PROJECT_CONFIG_KEY: project_id}
    if plan.explicit_forge:
        config["forge"] = forge

    try:
        if plan.datasource_id:
            datasource = await adopt_kb_connector_as_vault(
                plan, project_id, config, dependencies=dependencies
            )
        elif datasource_ref and datasource_ref.get("id"):
            datasource_id = str(datasource_ref["id"])
            updated = await store.update_datasource(
                datasource_id,
                credentials=credentials,
                config=config,
            )
            if not updated:
                raise RuntimeError("Native project KB datasource update failed")
            datasource = await store.get_datasource(datasource_id)
            if datasource is None:
                raise RuntimeError("Native project KB datasource disappeared")
        else:
            datasource = await store.create_datasource(
                name=f"{project['name']} Knowledge ({id8})",
                ds_type="kb",
                connection_url=None,
                description=(
                    f"This project's knowledge base, stored in `{repo_name}`. "
                    "Notes written with kb_write land here."
                ),
                credentials=credentials,
                config=config,
                created_by=owner_id,
                read_only=True,
                scope_mode="projects",
                auto_attach=True,
                project_ids=[project_id],
            )
    except Exception:
        repo_id = repo_row.get("id") if isinstance(repo_row, dict) else None
        if repo_id:
            try:
                await store.remove_project_repository(str(repo_id))
            except Exception:
                logger.exception(
                    "Failed to roll back external KB repository row for project %s",
                    project_id,
                )
        raise

    if plan.datasource_id:
        # The marker is stored, so the sweep has already let go of this row.
        # Whatever it indexed under its own UUID is now unreachable weight that
        # would double every search hit if the row were ever detached again —
        # disposable cleanup, never a reason to fail a provisioned project.
        try:
            await purge_kb_datasource_index(
                str(plan.datasource_id), dependencies=dependencies.knowledge_index
            )
        except Exception:
            logger.exception(
                "Failed to purge the external index of adopted KB connector %s",
                plan.datasource_id,
            )

    logger.info(
        "Attached external %s knowledge repo '%s' + KB connector %s to project %s",
        forge,
        repo_name,
        datasource["id"],
        project_id,
    )
    return repo_row, datasource


async def provision_default_project_knowledge(
    user: dict,
    project: dict,
    *,
    dependencies: ProjectProvisioningDependencies,
) -> None:
    """Create the dedicated knowledge vault for a user's default project."""
    try:
        if dependencies.forge.is_initialized and project:
            await provision_project_knowledge_repo(
                project, str(user["id"]), dependencies=dependencies
            )
    except Exception as e:
        dependencies.logger.warning(
            f"Failed to provision default-project knowledge for user {user['id']}: {e}"
        )
