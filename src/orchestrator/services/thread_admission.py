"""Persistent-session admission: the create funnel and the session list.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_ADMISSION``). ``POST /api/persistent/threads`` is the only path that raises
a session — ordinary, Officer, conference, review and protected-cloud alike —
so every refusal in it is a contract and none of them is re-derived here.

The operation is split along the seams it already had, not into helpers:

* :func:`build_session_config_override` — the per-session override rebuild.
  Pure: request fragment in, validated fragment plus the "ignored keys" warn
  list out. It never reads the store, so the rule that a nested ``llm``/
  ``delegation``/``workspace``/``tools``/``officer`` block is carried (or named
  as dropped) is testable on its own.
* :func:`select_thread_datasources` — the three-way selection origin
  (``explicit`` / ``default`` / ``omitted_compat``) plus its provenance stamp.
* :func:`resolve_thread_creation_plan` — every admission decision and every
  refusal, ending in the exact ``create_thread`` INSERT arguments. Nothing here
  has a side effect outside the request.
* :func:`commit_thread_creation` — the INSERT and the durable registrations
  that must not be observable before it (Officer post claim, conference hold,
  virtual binding, mounts, protected engage).
* :func:`provision_thread_workspace`, :func:`setup_thread_gitea`,
  :func:`setup_thread_main_cloud`, :func:`schedule_thread_agent` — the
  actuation forks, in their original order.

Ordering and fail-closed properties preserved literally:

* **Scope is authorized before anything else is read.** An invalid project
  request fails before any account, expert or connector work happens.
* **Physical state is materialized into the request layer once.** The resolved
  workspace backend and the resolved Officer/conference class are frozen into
  ``config_override`` at create, so a later expert or account edit cannot make
  the persisted runtime disagree with the workspace already provisioned or move
  a live thread onto a different wake plane.
* **Every protected-cloud incompatibility is a 422 with its own code** —
  ``protected_cloud_unsupported_workspace`` and
  ``protected_cloud_unsupported_session_class`` — and a protected session is
  pinned to the dedicated lane regardless of the stateless gate.
* **Officer authority is refused twice.** The pre-INSERT check avoids minting a
  thread that would immediately stand down; the atomic
  ``register_project_officer_thread`` claim after the INSERT is the authority,
  and a rival that slipped past the pre-check is stood down through the injected
  End funnel (B09) rather than a re-implemented retirement.
* **Gitea and main-cloud setup are awaited before the agent is assigned**, so
  the agent's workspace-readiness poll can never observe ``ready`` before
  ``git_remote_url`` exists.

Every collaborator arrives per invocation through
:class:`ThreadAdmissionDependencies`. That is not ceremony: ``postgres_db``, the
provisioner singletons, the feature gates and a dozen main-owned callables are
rebound by tests on ``orchestrator.main``, and several are owned by other
batches entirely (B09 End, B07 Officer conferences, B05 lane scheduling and
grants). Resolving any of them in this module's namespace would make a rebind on
main green but inert (port contract §P3).
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol
from uuid import uuid4

from fastapi import HTTPException, Request

from orchestrator.database.postgres import (
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
)
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.security.access import redact_config_override
from orchestrator.services.config_overrides import validated_config_name
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.default_experts import (
    DefaultExpertUnavailable,
    ExpertSelectionError,
    resolve_root_expert,
)
from orchestrator.services.job_datasource_selection import (
    datasource_selection_provenance,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    create_managed_repository,
    ensure_managed_repository_authority,
    revoke_and_delete_managed_repository,
)
from orchestrator.services.manifest_runtime_ownership import (
    require_srw_expert_configuration,
)
from orchestrator.services.session_class_policy import (
    materialized_session_class_override,
)
from orchestrator.services.session_create_overrides import (
    SessionOverrideError,
    bridge_nested_delegation_override,
    bridge_nested_llm_override,
    effective_officer_post_owned_refusal,
    ignored_override_paths,
    validated_reasoning_level,
    validated_session_officer_override,
)
from orchestrator.services.session_runtime_admission import thread_runtime_authority
from orchestrator.services.session_tool_policy import validated_tool_overrides
from orchestrator.services.session_workspace_policy import (
    validated_session_workspace_override,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.thread_project_authorization import (
    ThreadProjectAuthorizationDependencies,
    thread_creation_project_ids,
)
from orchestrator.services.workspace_binding import (
    ensure_virtual_thread_workspace_binding,
)
from orchestrator.services.workspace_tier_policy import backend_from_override
from shared.backend_kinds import LITE_BACKENDS
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


class ThreadAdmissionStore(Protocol):
    async def get_user_settings(self, user_id: str) -> dict[str, Any] | None: ...

    async def get_thread(self, thread_id: str) -> dict[str, Any] | None: ...

    async def create_thread(self, **kwargs: Any) -> str: ...

    async def get_officer_thread_for_project(
        self, project_id: str
    ) -> dict[str, Any] | None: ...

    async def register_project_officer_thread(
        self, project_id: str, thread_id: str, **kwargs: Any
    ) -> dict[str, Any] | None: ...

    async def decommission_project_officer(
        self, project_id: str, thread_id: str, **kwargs: Any
    ) -> Any: ...

    async def replace_thread_mounts(
        self, thread_id: str, rows: list[dict[str, Any]]
    ) -> Any: ...

    async def list_thread_mounts(self, thread_id: str) -> list[dict[str, Any]]: ...

    async def list_thread_mounts_bulk(
        self, thread_ids: list[str]
    ) -> dict[str, list[dict[str, Any]]]: ...

    async def list_threads(self, **kwargs: Any) -> list[dict[str, Any]]: ...

    async def merge_thread_workspace_context(
        self, thread_id: str, patch: dict[str, Any]
    ) -> Any: ...

    async def bind_thread_managed_repository(
        self, thread_id: str, *, repo_name: str, clean_url: str
    ) -> bool: ...

    async def update_thread_main_cloud(self, thread_id: str, **kwargs: Any) -> Any: ...

    def thread_advisory_lock(self, thread_id: str) -> Any: ...


@dataclass(frozen=True)
class ThreadAdmissionDependencies:
    """Collaborators for one session-admission request, resolved per invocation.

    ``store`` is main's ``postgres_db``; the six client/provisioner fields are
    its singletons. Everything else is a main-owned callable, grouped by why it
    cannot simply be imported here:

    *Feature gates* (§P1 — passed as callables so the live value is read):
    ``is_experts_db_enabled``, ``user_experts_enabled``,
    ``datasource_defaults_on_omission``, ``is_protected_cloud_mode_enabled``.

    *Other batches' authorities* (§P2/§P10 — consumed, never re-implemented):
    ``end_thread_flow`` (B09 retirement), ``can_manage_project_officer`` /
    ``find_open_conference_thread`` / ``inherit_conference_brain`` /
    ``hold_officer_for_conference`` / ``provision_commissioned_officer`` /
    ``validated_post_owned_officer_create_fragment`` /
    ``enforce_officer_auto_pull_release`` (B07 Officer),
    ``schedule_stateless_workspace_ensure`` (B05 request-path scheduling),
    ``schedule_protected_engage`` / ``record_protected_error`` (B04 cloud).

    *B05 policy reached through main's dependency objects* — the service needs
    a dependency argument this module has no business building:
    ``resolve_session_account_defaults``, ``prefetch_roster_refs``,
    ``resolve_thread_execution_lane``, ``build_thread_mount_rows``,
    ``should_skip_session_folder``, ``enforce_session_create_grants``,
    ``check_vm_permission``, ``resolve_cloud_session_url``.

    *Sibling admission policy*, injected rather than called directly so a test
    that rebinds it on main still steers this path:
    ``authorize_thread_project_ids``, ``authorize_thread_datasource_selection``.

    *Guards and the rest of the funnel*: ``enforce_readiness_gate``,
    ``require_approved_user``, ``find_idle_persistent_agent``,
    ``send_session_attach``, ``redact_thread_metadata``.
    """

    store: ThreadAdmissionStore
    gitea_client: Any
    main_cloud_router: Any
    agent_provisioner: Any
    container_provisioner: Any
    docker_provisioner: Any
    persistent_provisioner: Any
    vm_provisioner: Any

    enforce_readiness_gate: Callable[[], Awaitable[None]]
    require_approved_user: Callable[..., Awaitable[dict[str, Any]]]

    is_experts_db_enabled: Callable[[], bool]
    user_experts_enabled: Callable[[], Awaitable[bool]]
    datasource_defaults_on_omission: Callable[[], bool]
    is_protected_cloud_mode_enabled: Callable[[], bool]

    authorize_thread_project_ids: Callable[..., Awaitable[list[str]]]
    authorize_thread_datasource_selection: Callable[
        ..., Awaitable[tuple[list[str], dict[str, int]]]
    ]

    resolve_session_account_defaults: Callable[..., Awaitable[dict[str, Any]]]
    prefetch_roster_refs: Callable[..., Awaitable[Any]]
    resolve_thread_execution_lane: Callable[..., str]
    build_thread_mount_rows: Callable[..., Awaitable[list[dict[str, Any]]]]
    should_skip_session_folder: Callable[[list[dict[str, Any]]], bool]
    enforce_session_create_grants: Callable[..., Awaitable[Any]]
    check_vm_permission: Callable[..., Awaitable[Any]]
    resolve_cloud_session_url: Callable[..., str | None]

    validated_post_owned_officer_create_fragment: Callable[[Any], dict[str, Any] | None]
    enforce_officer_auto_pull_release: Callable[[Any], None]
    can_manage_project_officer: Callable[..., Awaitable[bool]]
    find_open_conference_thread: Callable[[str], Awaitable[dict[str, Any] | None]]
    inherit_conference_brain: Callable[..., list[str]]
    hold_officer_for_conference: Callable[..., Awaitable[None]]
    provision_commissioned_officer: Callable[..., Awaitable[None]]
    end_thread_flow: Callable[..., Awaitable[dict[str, Any]]]

    schedule_stateless_workspace_ensure: Callable[[str], Any]
    schedule_protected_engage: Callable[..., Any]
    record_protected_error: Callable[..., Awaitable[None]]

    find_idle_persistent_agent: Callable[[], Awaitable[dict[str, Any] | None]]
    send_session_attach: Callable[..., Awaitable[bool]]
    provision_or_assign: Callable[..., Awaitable[None]]
    redact_thread_metadata: Callable[[dict[str, Any]], dict[str, Any]]

    def project_dependencies(self) -> ThreadProjectAuthorizationDependencies:
        """Dependencies for the sibling project-policy module."""
        return ThreadProjectAuthorizationDependencies(store=self.store)


@dataclass(frozen=True)
class ThreadCreationPlan:
    """Everything admission decided, and nothing it did.

    Built by :func:`resolve_thread_creation_plan` and consumed by the commit and
    actuation steps. It is frozen because the INSERT arguments and the resolved
    physical class must not be re-derived downstream (port contract §P6).
    """

    config_name: str
    config_override: dict[str, Any]
    effective_create_config: dict[str, Any]
    effective_project_ids: list[str]
    primary_project_id: str | None
    selected_datasource_ids: list[str]
    execution_lane: str
    thread_backend: str | None
    lite_session: bool
    vm_session: bool
    use_k8s: bool
    create_kwargs: dict[str, Any]
    ignored_override_keys: list[str] = field(default_factory=list)
    officer_requested: bool = False
    explicit_officer_commission: bool = False


def build_session_config_override(
    request_body: ThreadCreateRequest,
    *,
    user_id: str,
    dependencies: ThreadAdmissionDependencies,
) -> tuple[dict[str, Any], list[str]]:
    """Rebuild the explicit per-session override layer from one create request.

    Returns ``(config_override, ignored_override_keys)``. The warn list is
    computed BEFORE the server-owned Officer Post fragment is folded in, so a
    trusted commission never shows up as a key the caller sent.
    """
    config_override: dict[str, Any] = {}

    # Per-session overrides from request (take priority over user defaults)
    if request_body.model:
        config_override.setdefault("llm", {})["model"] = request_body.model
    if request_body.temperature is not None:
        config_override.setdefault("llm", {})["temperature"] = request_body.temperature
    if request_body.reasoning_level:
        # Same bridge as model/temperature — without it a requested effort
        # would be silently dropped by this validated-fragments rebuild
        # (the exact trap that once ate the officer block). Vocabulary
        # check here; the family capability still clamps at attach.
        config_override.setdefault("llm", {})["reasoning_level"] = (
            validated_reasoning_level(request_body.reasoning_level)
        )
    # The same three LLM keys may arrive NESTED under config_override.llm
    # — that is how the New Session form sends reasoning_level and
    # temperature (it lifts only model + permission_mode to top-level
    # fields), and how API/MCP callers naturally write them. The rebuild
    # above never read that shape, so a create-time "max" was dropped on
    # every ordinary session. Top-level fields keep winning; a malformed
    # nested value is a 400 here, never a silent drop.
    try:
        bridge_nested_llm_override(
            request_body.config_override,
            config_override,
            validate_reasoning_level=validated_reasoning_level,
        )
        # The Delegation toggle writes `tools.delegation` (names) AND
        # `delegation.enabled` (the explicit-grant gate); the tools half
        # is bridged below, this carries the gate — without it the agent
        # logs "configured tool(s) did not bind" for all five.
        bridge_nested_delegation_override(request_body.config_override, config_override)
    except SessionOverrideError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    # The agent reads its permission mode from config.interactive.permission_mode
    # (src/api/persistent_session.py), NOT from the threads.permission_mode
    # column — so a per-session choice only reaches the agent if it lands in
    # config_override, exactly like the model/temperature bridges above and the
    # runtime PATCH path (agent_update_thread_config). Without this, picking a
    # non-default mode in the New Session form was silently dropped and every
    # session booted "supervised". Field is str|None: omitted → keep the user
    # default applied above; present → it wins.
    if request_body.permission_mode:
        config_override.setdefault("interactive", {})["permission_mode"] = (
            request_body.permission_mode
        )

    # Per-session WORKSPACE TIER from the New Session "Backend" selector.
    # The cockpit sends it nested under request_body.config_override
    # ({"workspace": {"backend": ..., "max_read_words": ..., ...}}).
    # ThreadCreateRequest historically declared no config_override field, so
    # Pydantic dropped it and create_thread rebuilt the override only from
    # model/temperature/permission_mode — every session booted the default
    # (sandbox) regardless of the dropdown, because the provisioning fork
    # below keys off backend_from_override(config_override). Honor the
    # validated workspace sub-dict here (no creds); the backend must land in
    # config_override now because the workspace is provisioned synchronously
    # at create — unlike the other Advanced settings it can't be a runtime
    # PATCH. Tool groups are validated against the registry just below.
    req_workspace = validated_session_workspace_override(request_body.config_override)
    if req_workspace:
        config_override.setdefault("workspace", {}).update(req_workspace)
    # EVERY tool category the request names, not four of them. The form
    # renders twelve and writes `tools.<cat>: []` for each one unticked;
    # copying across only the four allowlisted groups meant the other eight
    # restrictions were shown to the user, accepted by the API, and thrown
    # away. Names are checked against their own registry category, so a
    # smuggled `tools.canvas: ["run_command"]` is a 400 rather than a shell
    # tool. Grants are still enforced below on the fully resolved config.
    req_tool_groups = validated_tool_overrides(request_body.config_override)
    if req_tool_groups:
        config_override.setdefault("tools", {}).update(req_tool_groups)
    # Officer (centurion) block — must be denormalized into thread
    # metadata for the orchestrator's SQL machinery (centurion.md §4).
    req_officer = validated_session_officer_override(request_body.config_override)
    if req_officer:
        config_override.setdefault("officer", {}).update(req_officer)
    # Warn phase of a strict contract (KEP-2885 shape: Ignore → Warn →
    # Strict): every nested key the rebuild above did not carry is named
    # in the log and echoed on the response, so the next dropped field is
    # visible on day one instead of found in a session weeks later.
    ignored_override_keys = ignored_override_paths(
        request_body.config_override, config_override
    )
    if ignored_override_keys:
        logger.warning(
            "Thread create ignored config_override keys for user %s: %s",
            user_id[:8],
            ", ".join(ignored_override_keys),
        )
    trusted_post_officer = dependencies.validated_post_owned_officer_create_fragment(
        request_body._officer_post_config_snapshot
    )
    if trusted_post_officer is not None:
        # The only bridge for unattended/spend authority into a runtime.
        # It is derived from the exact durable snapshot whose generation is
        # revalidated under the Post lock at registration below.
        config_override.setdefault("officer", {}).update(trusted_post_officer)
    return config_override, ignored_override_keys


async def select_thread_datasources(
    request_body: ThreadCreateRequest,
    user: dict[str, Any],
    *,
    thread_backend: str | None,
    effective_project_ids: list[str],
    dependencies: ThreadAdmissionDependencies,
) -> tuple[list[str], dict[str, int], dict[str, Any]]:
    """Materialize one complete selection with the thread row.

    Cockpit sends a reviewed array (including ``[]``); omission is temporarily
    gated for older API clients that encoded an opt-out by leaving the field
    out. Returns ``(ids, policy_revisions, provenance)``.
    """
    if "datasource_ids" in request_body.model_fields_set:
        thread_selection_origin = "explicit"
        (
            selected_thread_datasource_ids,
            selected_thread_datasource_revisions,
        ) = await dependencies.authorize_thread_datasource_selection(
            user,
            request_body.datasource_ids or [],
            workspace_backend=thread_backend,
            target_project_ids=effective_project_ids,
            effective_work_owner_id=str(user["id"]),
        )
    elif (
        request_body.use_datasource_defaults
        or dependencies.datasource_defaults_on_omission()
    ):
        from orchestrator.services.datasource_policy import (
            DatasourceUnavailableError,
            default_datasource_selection,
        )

        thread_selection_origin = "default"
        try:
            (
                selected_thread_datasource_ids,
                selected_thread_datasource_revisions,
            ) = await default_datasource_selection(
                dependencies.store,
                str(user["id"]),
                effective_project_ids,
                thread_backend,
            )
        except DatasourceUnavailableError as exc:
            raise HTTPException(
                status_code=403,
                detail="One or more selected connectors are unavailable",
            ) from exc
    else:
        thread_selection_origin = "omitted_compat"
        selected_thread_datasource_ids = []
        selected_thread_datasource_revisions = {}

    thread_selection_provenance = await datasource_selection_provenance(
        datasource_ids=selected_thread_datasource_ids,
        policy_revisions=selected_thread_datasource_revisions,
        origin=thread_selection_origin,
        effective_work_owner_id=str(user["id"]),
        actor=user,
        project_ids=effective_project_ids,
        creation_path="persistent_thread_rest",
    )
    return (
        selected_thread_datasource_ids,
        selected_thread_datasource_revisions,
        thread_selection_provenance,
    )


async def resolve_thread_creation_plan(
    request_body: ThreadCreateRequest,
    user: dict[str, Any],
    *,
    dependencies: ThreadAdmissionDependencies,
) -> ThreadCreationPlan:
    """Every admission decision for one create request, and no side effect.

    Raises the create path's refusals in their original order: project scope,
    expert selection, override validity, Officer release/post authority,
    conference single-writer, protected-cloud compatibility, connector policy,
    VM availability and capability grants.
    """
    # Project scope is needed for both selection precedence and grant
    # resolution, so authorize it before choosing the expert.
    requested_project_ids = thread_creation_project_ids(request_body, user)
    effective_project_ids = await dependencies.authorize_thread_project_ids(
        user, requested_project_ids
    )
    # A project default/config override is safe only with one unambiguous
    # primary project. The legacy project_id field explicitly identifies
    # that primary; otherwise a multi-project session skips this layer.
    primary_project_id = (
        str(request_body.project_id)
        if request_body.project_id
        else (effective_project_ids[0] if len(effective_project_ids) == 1 else None)
    )

    # Account preferences are fallback values, not request overrides. Keep
    # them below the selected expert during both create-time provisioning
    # and every later attach. Fetch them only after scope authorization so
    # an invalid project request fails before any unrelated account work.
    all_user_settings = await dependencies.store.get_user_settings(str(user["id"]))
    account_defaults = await dependencies.resolve_session_account_defaults(
        str(user["id"]), all_user_settings or {}
    )

    # Write boundary: threads.config_name is read back by /resume, by the
    # Officer recycler and by the magic-link wake, all of which provision
    # from a fire-and-forget task with no request left to answer. Refuse a
    # value none of them could ever boot, here, before the INSERT.
    config_name = canonical_config_name(
        validated_config_name(request_body.config_name) or "session_base"
    )
    if request_body.expert_id and config_name != "session_base":
        raise HTTPException(
            status_code=400,
            detail=(
                "expert_id cannot be combined with a bundled session "
                "config_name; select one expert source"
            ),
        )
    selected_expert_id = request_body.expert_id
    selected_expert_row: dict[str, Any] | None = None
    project_expert_override: dict[str, Any] | None = None
    selection = None
    try:
        if (
            dependencies.is_experts_db_enabled()
            and await dependencies.user_experts_enabled()
            and (request_body.expert_id or config_name == "session_base")
        ):
            selection = await resolve_root_expert(
                dependencies.store,
                expert_type="session",
                user_id=str(user["id"]),
                project_id=primary_project_id,
                explicit_expert_id=request_body.expert_id,
                is_admin=bool(user.get("is_admin")),
            )
            selected_expert_row = selection.expert
            require_srw_expert_configuration(selected_expert_row, interactive=True)
            selected_expert_id = str(selection.expert["id"])
            project_expert_override = selection.project_override
            config_name = "session_base"
    except ExpertSelectionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except DefaultExpertUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    config_override, ignored_override_keys = build_session_config_override(
        request_body, user_id=str(user["id"]), dependencies=dependencies
    )

    from orchestrator.services.manifest_workspace_selection import (
        select_execution_workspace,
    )
    from shared.runtime.core.workspace_selection import bind_execution_workspace

    selected_workspace, workspace_selection = await select_execution_workspace(
        dependencies.store,
        user,
        project_id=primary_project_id,
        role="session",
        workspace=request_body.workspace,
        supplied="workspace" in request_body.model_fields_set,
        config_override=config_override,
        account_defaults=account_defaults,
    )
    config_override = bind_execution_workspace(config_override, selected_workspace)

    # Resolve the complete create-time policy view.  This is also the source
    # for infrastructure-affecting values and grants, preventing the create
    # path from validating only a thin request fragment while attach sees a
    # broader expert/base config.
    create_capture: dict[str, Any] = {}
    resolve_config(
        base_config_name=config_name,
        base_defaults=account_defaults,
        expert_row=selected_expert_row,
        project_overrides=project_expert_override,
        request_override=config_override or None,
        expert_type="session",
        capture=create_capture,
        db_refs=await dependencies.prefetch_roster_refs(
            expert_row=selected_expert_row,
            overrides=(project_expert_override, config_override),
            user_id=str(user["id"]),
            project_ids=[primary_project_id] if primary_project_id else [],
        ),
    )
    effective_create_config = create_capture["merged_fragment"]

    effective_class = materialized_session_class_override(effective_create_config)
    if effective_class["enabled"]:
        effective_officer = effective_create_config.get("officer") or {}
        effective_auto_pull = (
            effective_officer.get("auto_pull")
            if isinstance(effective_officer, dict)
            else None
        )
        if effective_auto_pull not in (None, False):
            # This check is intentionally on the fully resolved config, so
            # an account, expert, or project default cannot bypass the
            # deployment release fence.
            dependencies.enforce_officer_auto_pull_release(effective_auto_pull)
        if request_body._officer_post_config_snapshot is None:
            refusal = effective_officer_post_owned_refusal(effective_create_config)
            if refusal is not None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"officer.{refusal} is owned by the durable Officer "
                        "Post; use the project Officer endpoint"
                    ),
                )
            # Materialize the safe value in the request layer. This keeps
            # a later attach from acquiring a mutable inherited setting.
            config_override.setdefault("officer", {})["auto_pull"] = False

    # A workspace tier is physical session state. Materialize the resolved
    # choice once so later edits to an expert/account default cannot make the
    # persisted runtime disagree with the workspace already provisioned.
    effective_backend = backend_from_override(effective_create_config)
    if effective_backend:
        config_override.setdefault("workspace", {})["backend"] = effective_backend
    if request_body.protected_cloud and effective_backend != "sandbox":
        raise HTTPException(
            status_code=422,
            detail={
                "code": "protected_cloud_unsupported_workspace",
                "message": (
                    "Protected cloud sessions require the Container workspace tier."
                ),
            },
        )

    # Officer/conference selects pinned-only lifecycle machinery. Freeze
    # the fully resolved booleans into the highest-priority request layer
    # just like the physical workspace tier above. Later expert/account
    # edits can still update ordinary config, but cannot silently move an
    # existing stateless thread onto a different wake plane.
    materialized_session_class = materialized_session_class_override(
        effective_create_config
    )
    config_override.setdefault("officer", {}).update(materialized_session_class)
    if request_body.protected_cloud and materialized_session_class["enabled"]:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "protected_cloud_unsupported_session_class",
                "message": (
                    "Protected cloud sessions are not supported for the "
                    "background Officer runtime."
                ),
            },
        )

    # Conference embodiment (centurion.md §2/S9): validate the MATERIALIZED
    # effective class, not only the user's explicit fragment. An expert or
    # account default may select conference too, and must obey the same
    # single-project/single-writer rules.
    if (config_override.get("officer") or {}).get("conference") is True:
        if not primary_project_id:
            raise HTTPException(
                status_code=400,
                detail="A conference session needs exactly one project — "
                "the officer's identity is project-scoped.",
            )
        # A conference is an Officer-management mutation: opening it holds
        # the background Officer.  Project attachment alone admits viewers
        # and editors to ordinary sessions, so enforce the current
        # owner/admin authority again here before that side effect.  This
        # is the server fence for a stale card whose role was just revoked.
        if not await dependencies.can_manage_project_officer(user, primary_project_id):
            raise HTTPException(
                status_code=403,
                detail="Project owner role required to open an Officer conference",
            )
        _open_conf = await dependencies.find_open_conference_thread(primary_project_id)
        if _open_conf:
            raise HTTPException(
                status_code=409,
                detail=(
                    "conference_open: this project already has an open "
                    f"conference session ({_open_conf['id']}) — resume it "
                    "instead of opening a second one."
                ),
            )
        # His embodiment thinks with his brain (§3.1). Request keys were
        # bridged into config_override["llm"] above, so they win here.
        _standing_officer = await dependencies.store.get_officer_thread_for_project(
            primary_project_id
        )
        _inherited_brain = dependencies.inherit_conference_brain(
            config_override, _standing_officer
        )
        if _inherited_brain:
            logger.info(
                "conference on project %s inherits the officer's brain: %s",
                str(primary_project_id)[:8],
                ", ".join(_inherited_brain),
            )

    # Officer post admission (officer_post.md §4): the create funnel is
    # the only path that raises an officer, and the post admits one
    # incarnation at a time. Refuse BEFORE provisioning — the atomic
    # registration claim after the INSERT below is the authority; this
    # early check just avoids creating a thread we would immediately
    # have to stand down. Posts are project-scoped, so an officer class
    # materialized onto a project-less session (account/expert default)
    # has no post to claim and keeps its pre-post behavior: it creates,
    # unregistered — every project-keyed officer read already ignores it.
    _officer_requested = (config_override.get("officer") or {}).get("enabled") is True
    _explicit_officer_commission = (
        request_body._officer_post_config_snapshot is not None
    )
    if _officer_requested and primary_project_id:
        if not await dependencies.can_manage_project_officer(user, primary_project_id):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Project owner role required to commission an Officer; "
                    "use the project Officer endpoint"
                ),
            )
        if request_body._officer_post_config_snapshot is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A project Officer can only be commissioned through the "
                    "durable project Officer endpoint"
                ),
            )
        _standing = await dependencies.store.get_officer_thread_for_project(
            primary_project_id
        )
        if _standing:
            raise HTTPException(
                status_code=409,
                detail=(
                    "already commissioned: this project's post is held "
                    f"by thread {_standing['id']} — retire him before "
                    "raising another officer."
                ),
            )

    thread_backend = backend_from_override(config_override)
    (
        selected_thread_datasource_ids,
        selected_thread_datasource_revisions,
        thread_selection_provenance,
    ) = await select_thread_datasources(
        request_body,
        user,
        thread_backend=thread_backend,
        effective_project_ids=effective_project_ids,
        dependencies=dependencies,
    )
    # VM tier is operator-gated on top of the ``vm_workspace`` PDP grant that
    # enforce_session_create_grants runs below: the global ``vm_workspaces``
    # kill-switch + per-user ``can_use_vm`` (admins bypass). Fail fast with a
    # clear 503 when no VM provisioner is wired (e.g. local k3d) instead of
    # accepting the session and hanging the attach until its ready timeout.
    # Mirrors the session→VM upgrade gate (agent_upgrade_thread_to_vm).
    if backend_from_override(config_override) == "vm":
        await dependencies.check_vm_permission(user, job_needs_vm=True)
        if not dependencies.vm_provisioner.is_available:
            raise HTTPException(
                status_code=503,
                detail="VM provisioning is not available on this deployment",
            )

    # Layer 2 (fail loud at create): the fully resolved config must fit the
    # owner's capability grants. Reject a never-startable session with 422
    # NOW — before persisting/provisioning — instead of accepting it and
    # letting the attach pre-flight fail it later (Phase 1). Validates the
    # user-chosen overrides (permission_mode, model, workspace.backend,
    # tools) against the owner's grants + the session's project scope; admins
    # bypass. knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    await dependencies.enforce_session_create_grants(
        effective_create_config,
        user_id=str(user["id"]),
        project_ids=effective_project_ids,
    )

    # Keep the threads.permission_mode column in sync with the mode the
    # fully resolved config will load (request > expert > account > base).
    effective_permission_mode = (effective_create_config.get("interactive") or {}).get(
        "permission_mode"
    ) or "supervised"
    effective_narration_mode = (effective_create_config.get("interactive") or {}).get(
        "narration_mode"
    ) or "auto"

    execution_lane = dependencies.resolve_thread_execution_lane(
        workspace_backend=thread_backend,
        effective_config=effective_create_config,
    )
    if request_body.protected_cloud:
        # Protected-cloud overlay staging is not yet lease/runtime fenced;
        # keep it on the dedicated plane even when ordinary sandbox
        # sessions are admitted to the stateless pool.
        execution_lane = "pinned"

    # Store config_override + datasource ids in thread metadata. Project
    # attachment is the canonical concern of ``thread_mounts`` (Phase 1
    # of cloud_collaboration_model.md §9) — the legacy
    # ``metadata.project_ids`` JSONB key is no longer written.
    metadata_patch = {}
    if config_override:
        # This is the explicit per-session layer only. Account fallback and
        # expert fields are re-resolved at attach; credentials are injected
        # into the delivery blob and never stored in the thread row.
        metadata_patch["config_override"] = redact_config_override(config_override)
    if selected_expert_id:
        # Persist the actual selected row, including application/personal/
        # project fallthroughs, so pointer changes affect only new sessions.
        metadata_patch["expert_id"] = selected_expert_id
        metadata_patch["expert_selection_source"] = (
            selection.source if selection else "explicit"
        )
    if request_body.protected_cloud:
        metadata_patch["protected_cloud"] = True
    trusted_seed = request_body._trusted_seed
    if trusted_seed is not None:
        # Keep this narrow and auditable. A future server-owned seed needs
        # an explicit design decision instead of acquiring an arbitrary
        # metadata write channel through this private attribute.
        if set(trusted_seed.metadata) != {"review_delivery"}:
            raise RuntimeError("Unsupported trusted thread seed")
        metadata_patch.update(copy.deepcopy(trusted_seed.metadata))

    # Decide physical actuation before the INSERT. Every stateless thread
    # commits its materialized class/tier in that transaction; a K8s thread
    # additionally commits its one-shot create nonce. A crash after INSERT
    # can then be reconciled safely instead of leaving an unclassified or
    # ambiguous markerless row.
    lite_session = thread_backend in LITE_BACKENDS
    vm_session = thread_backend == "vm"
    use_k8s = (
        not lite_session
        and not vm_session
        and dependencies.container_provisioner.is_available
        and (
            dependencies.container_provisioner.in_cluster
            or not dependencies.docker_provisioner.is_available
        )
    )
    stateless_initial_metadata = execution_lane == "stateless"
    if stateless_initial_metadata and use_k8s:
        metadata_patch["workspace_container"] = {
            "status": "pending",
            "provisioner": "k8s",
            "_runtime_creation": {
                "generation": str(uuid4()),
                "mode": "create",
                "attempted": False,
                "replaces_uid": None,
            },
        }

    create_kwargs = dict(
        user_id=str(user["id"]),
        project_id=primary_project_id,
        config_name=config_name,
        permission_mode=effective_permission_mode,
        narration_mode=effective_narration_mode,
        title=request_body.title,
        datasource_ids=selected_thread_datasource_ids,
        datasource_selection_provenance=thread_selection_provenance,
        datasource_policy_revisions=selected_thread_datasource_revisions,
        authority_user_id=str(user["id"]),
        authority_project_ids=effective_project_ids,
        execution_lane=execution_lane,
    )
    if workspace_selection is not None:
        create_kwargs["workspace_selection"] = workspace_selection
    if trusted_seed is not None:
        create_kwargs["initial_event"] = trusted_seed.opening_event
    # A review branch is attach authority, not decorative metadata. Commit
    # it in the thread INSERT transaction together with the opening event,
    # even on the pinned lane, so no reconciler can observe a created review
    # thread before its exact delivery constraint exists.
    create_kwargs["initial_metadata"] = metadata_patch

    return ThreadCreationPlan(
        config_name=config_name,
        config_override=config_override,
        effective_create_config=effective_create_config,
        effective_project_ids=effective_project_ids,
        primary_project_id=primary_project_id,
        selected_datasource_ids=selected_thread_datasource_ids,
        execution_lane=execution_lane,
        thread_backend=thread_backend,
        lite_session=lite_session,
        vm_session=vm_session,
        use_k8s=use_k8s,
        create_kwargs=create_kwargs,
        ignored_override_keys=ignored_override_keys,
        officer_requested=_officer_requested,
        explicit_officer_commission=_explicit_officer_commission,
    )


async def commit_thread_creation(
    plan: ThreadCreationPlan,
    request_body: ThreadCreateRequest,
    user: dict[str, Any],
    *,
    dependencies: ThreadAdmissionDependencies,
) -> tuple[str, Any]:
    """Insert the thread and commit every registration bound to that INSERT.

    Returns ``(thread_id, created_runtime_authority)``. The Officer post claim
    is the authority: a rival create that slipped past the pre-check loses here
    and the thread it minted is stood down through the injected End funnel, so
    the JSONB-predicate machinery never sees two live officers.
    """
    store = dependencies.store
    thread_id = await store.create_thread(**plan.create_kwargs)
    created_runtime_authority = thread_runtime_authority(
        await store.get_thread(str(thread_id))
    )
    if created_runtime_authority is None:
        raise HTTPException(
            status_code=409,
            detail="Thread runtime changed during create admission",
        )

    config_override = plan.config_override
    primary_project_id = plan.primary_project_id

    # Officer post registration (officer_post.md §4): link the new
    # incarnation on the project's post so the row can never disagree
    # with the threads table about who holds it. The claim is atomic — a
    # rival create that slipped past the pre-check loses here, and the
    # thread it minted is stood down so the JSONB-predicate machinery
    # (wake claim, watchdog) never sees two live officers.
    if plan.officer_requested and primary_project_id:
        _registered = await store.register_project_officer_thread(
            primary_project_id,
            str(thread_id),
            config_override=redact_config_override(config_override),
            expected_post_config_override=(request_body._officer_post_config_snapshot),
            commission_continuity=plan.explicit_officer_commission,
        )
        if _registered is None:
            try:
                race_loser = await store.get_thread(str(thread_id))
                if race_loser is not None:
                    await dependencies.end_thread_flow(
                        str(thread_id),
                        race_loser,
                        permanent=False,
                        force=True,
                        officer_retire_reason="commission_race_lost",
                    )
                else:
                    await store.decommission_project_officer(
                        primary_project_id,
                        str(thread_id),
                        reason="commission_race_lost",
                        force=True,
                        allow_orphan_retirement=True,
                    )
            except Exception:
                logger.warning(
                    "officer registration race: stand-down of thread %s failed",
                    thread_id,
                )
            raise HTTPException(
                status_code=409,
                detail="already commissioned: this project's post was "
                "claimed by a concurrent officer create.",
            )
        if plan.explicit_officer_commission:
            request_body._officer_commission_result = _registered.get(
                "commission_continuity"
            )

    # Conference open → hold the background officer (centurion.md §4):
    # events and timers queue durably until the brief wake at conference
    # end. No-op on officer-less projects.
    if (
        primary_project_id
        and (config_override.get("officer") or {}).get("conference") is True
    ):
        await dependencies.hold_officer_for_conference(primary_project_id, thread_id)

    # A durable virtual object-store namespace is itself the workspace
    # backing. Bind it once at thread creation and preserve that generation
    # across agent-pod restarts. Process-local memory is deliberately not
    # bound/advertised to Canvas because another orchestrator replica cannot
    # read it.
    if backend_from_override(config_override) == "virtual":
        await ensure_virtual_thread_workspace_binding(store, thread_id)

    # Seed thread_mounts for the attached projects.
    if plan.effective_project_ids:
        try:
            mount_rows = await dependencies.build_thread_mount_rows(
                plan.effective_project_ids
            )
            if mount_rows:
                await store.replace_thread_mounts(thread_id, mount_rows)
        except Exception as e:
            logger.warning("Thread %s: failed to seed thread_mounts: %s", thread_id, e)

    # Engage protected cloud mode ONCE at create (design §3.3/§11.4), fire-
    # and-forget so create latency is unaffected — mirrors
    # ``provision_thread_workspace`` below. Fail-closed: a refusal or
    # provisioning error is recorded on the thread's metadata inside
    # ``_engage_protected_cloud_for_thread`` itself and never raises here;
    # the session simply boots with no cloud mount. Registered via
    # ``schedule_protected_engage`` (F-I1) so a concurrent attach that
    # lands before this task finishes can await it instead of racing it.
    if request_body.protected_cloud:
        if dependencies.is_protected_cloud_mode_enabled():
            seeded_rows = await store.list_thread_mounts(thread_id)
            engage_thread = await store.get_thread(thread_id)
            engage_authority = thread_runtime_authority(engage_thread)
            if engage_authority is None:
                raise HTTPException(
                    status_code=409,
                    detail="Thread runtime changed before protected engage",
                )
            dependencies.schedule_protected_engage(
                thread_id,
                user_id=str(user["id"]),
                mount_rows=seeded_rows,
                runtime_generation=engage_authority.generation,
            )
        else:
            # F-M2: explain the degradation up front instead of leaving a
            # protected-marked, mount-less thread silent about why.
            engage_thread = await store.get_thread(thread_id)
            engage_authority = thread_runtime_authority(engage_thread)
            if engage_authority is not None:
                await dependencies.record_protected_error(
                    thread_id,
                    "protected cloud mode is disabled on this deployment",
                    code="feature_disabled",
                    expected_runtime_generation=engage_authority.generation,
                )

    return str(thread_id), created_runtime_authority


async def _provision_thread_vm(
    tid: str,
    cfg: str,
    config_override: dict,
    *,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    """Install the VM provision intent under the thread's current authority."""
    from orchestrator.services.vm_workspace_config import vm_provisioning_options

    store = dependencies.store
    vm_provisioner = dependencies.vm_provisioner
    try:
        async with store.thread_advisory_lock(tid):
            current = await store.get_thread(tid)
            current_authority = thread_runtime_authority(current)
            current_metadata = thread_metadata_object(current or {})
            raw_vm = current_metadata.get("vm")
            if raw_vm is not None and not isinstance(raw_vm, Mapping):
                ok = False
            elif current_authority is None:
                ok = False
            else:
                options = await vm_provisioning_options(
                    store,
                    "Session",
                    current,
                    fallback=current_metadata.get("config_override", config_override),
                )
                ok = await vm_provisioner.create_thread_vm(
                    thread_id=tid,
                    agent_config=cfg,
                    **options,
                    expected_runtime_generation=(current_authority.generation),
                    expected_agent_id=(
                        str(current["agent_id"])
                        if current and current.get("agent_id") is not None
                        else None
                    ),
                    expected_attach_token=(
                        str(current["runtime_attach_token"])
                        if current and current.get("runtime_attach_token") is not None
                        else None
                    ),
                    expected_vm_context=(dict(raw_vm) if raw_vm is not None else None),
                )
    except Exception:
        logger.exception("Thread %s: VM provisioning request raised", tid)
        ok = False
    if not ok:
        # The provisioner generation-fences failures only after a
        # successful intent install.  A stale/retired caller must
        # not mutate the current thread merely to surface an old
        # request failure.
        logger.warning(
            "Thread %s: VM provisioning authority was not admitted",
            tid,
        )


async def _provision_thread_workspace_container(
    tid: str, *, dependencies: ThreadAdmissionDependencies
) -> None:
    ok = await dependencies.container_provisioner.create_pinned_thread_workspace(tid)
    if not ok:
        logger.error(
            "Thread %s: workspace container provisioning failed. "
            "Check image availability, RBAC, and node resources.",
            tid,
        )


async def _assign_thread_workspace(
    tid: str, *, dependencies: ThreadAdmissionDependencies
) -> None:
    result = await dependencies.docker_provisioner.assign_thread_workspace(tid)
    if not result:
        logger.warning(
            "Thread %s: no free workspace in Docker pool. All containers occupied.",
            tid,
        )


async def provision_thread_workspace(
    plan: ThreadCreationPlan,
    thread_id: str,
    *,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    """Start the workspace backing for a freshly created thread (non-blocking).

    Provision workspace container + agent pod FIRST (non-blocking).
    Start image pull / pod creation immediately so it runs in parallel
    with the Gitea + Nextcloud setup below.
    Same priority as dispatcher: in-cluster K8s → Docker Compose → kubeconfig K8s
    Lite (virtual/none) sessions run with no workspace pod — skip every
    provisioning path below (no_workspace_agent_mode.md §4). The session
    agent builds its lite backend from the mounts injected at attach.
    """
    config_override = plan.config_override
    if plan.lite_session:
        logger.info(
            "Thread %s: lite workspace backend — no workspace pod provisioned",
            thread_id,
        )
    elif plan.vm_session:
        # VM tier: the workspace is a KubeVirt VM (metadata.vm), not a
        # sandbox container. Mark it provisioning SYNCHRONOUSLY so the agent's
        # attach-time workspace poll (_poll_workspace_ready) observes a VM in
        # flight (vm_status truthy) and waits on the VM budget instead of
        # bailing "no workspace provisioned". Then fire create_thread_vm
        # fire-and-forget (mirrors the container task) with the requested
        # sizing; the agent pod provisioned below SSHes into the VM once it
        # reports ready. (knowledge-base/knowledge/features/session_create_on_vm.md)
        asyncio.create_task(
            _provision_thread_vm(
                thread_id,
                plan.config_name,
                config_override,
                dependencies=dependencies,
            )
        )
    elif plan.use_k8s:
        if plan.execution_lane == "stateless":
            # Stateless create shares the same distributed lifecycle owner
            # as input, resume, attach-poll recovery, and terminal cleanup.
            # A direct provisioner task could otherwise outlive a public
            # End and recreate/publish a pod after retirement completed.
            dependencies.schedule_stateless_workspace_ensure(thread_id)
        else:
            asyncio.create_task(
                _provision_thread_workspace_container(
                    thread_id, dependencies=dependencies
                )
            )
    elif dependencies.docker_provisioner.is_available:
        await dependencies.store.merge_thread_workspace_context(
            thread_id, {"status": "pending"}
        )

        # Docker Compose mode: assign from static pool
        asyncio.create_task(
            _assign_thread_workspace(thread_id, dependencies=dependencies)
        )
    else:
        logger.warning(
            "Thread %s: workspace container not provisioned — "
            "no provisioner available. "
            "Start the agent manually: python -m agent --mode persistent "
            "--thread-id %s",
            thread_id,
            thread_id,
        )


async def setup_thread_gitea(
    thread_id: str,
    user: dict[str, Any],
    *,
    primary_project_id: str | None,
    lite_session: bool,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    """Create and bind the thread's scoped managed repository."""
    gitea_client = dependencies.gitea_client
    store = dependencies.store
    if lite_session:
        return
    if not gitea_client.is_initialized and gitea_client.is_configured:
        await gitea_client.ensure_initialized()
    if not gitea_client.is_initialized:
        return
    repo_name = f"thread-{thread_id[:8]}"
    try:
        git_remote_url, creation_intent = await create_managed_repository(
            store,
            gitea_client,
            repo_name=repo_name,
            authority_kind="thread",
            authority_id=thread_id,
            project_id=primary_project_id,
            access_mode="write",
        )
        if git_remote_url:
            repository_authority = await ensure_managed_repository_authority(
                store,
                gitea_client,
                repo_name=repo_name,
                authority_kind="thread",
                authority_id=thread_id,
                project_id=primary_project_id,
                access_mode="write",
                creation_intent_id=str(creation_intent["id"]),
            )
        if not await store.bind_thread_managed_repository(
            thread_id,
            repo_name=repo_name,
            clean_url=str(repository_authority["clean_repo_url"]),
        ):
            await revoke_and_delete_managed_repository(store, gitea_client, repo_name)
            raise HTTPException(
                status_code=503,
                detail="Scoped workspace repository binding failed",
            )
        if user.get("email"):
            try:
                # Pass username + full_name + sub so grant_user_repo_access
                # can pre-provision the Gitea user if they haven't
                # visited Gitea directly yet. sub is used as login_name
                # so Gitea's OIDC matches this account on first direct
                # login instead of creating a duplicate.
                email_local = user["email"].split("@")[0]
                await gitea_client.grant_user_repo_access(
                    user["email"],
                    repo_name,
                    username=user.get("preferred_username") or email_local,
                    full_name=user.get("display_name"),
                    sub=user.get("keycloak_sub"),
                )
            except Exception as e:
                logger.warning(
                    "Failed to grant Gitea access for thread %s: %s",
                    thread_id,
                    e,
                )
    except ManagedRepositoryAuthorityError as exc:
        await revoke_and_delete_managed_repository(store, gitea_client, repo_name)
        raise HTTPException(
            status_code=503,
            detail="Scoped workspace repository authority unavailable",
        ) from exc


async def setup_thread_main_cloud(
    thread_id: str,
    user: dict[str, Any],
    *,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    """Provision (or deliberately skip) the legacy main-cloud session folder."""
    # Fresh session folder for a new thread — resolve via the owner
    # seam (active today). The thread row is stamped with this
    # backend's id below, so resume/delete later dispatch via
    # for_thread. Issue 16, knowledge-base/knowledge/issues/main_cloud.md.
    # Main-cloud storage is optional.  ``for_owner`` deliberately
    # fails closed when no durable active-instance authority exists,
    # because callers that intend a cloud effect must never fall back
    # to an unattested adapter.  Thread creation itself is not such an
    # effect: in a no-cloud deployment it must continue without the
    # legacy session folder.
    store = dependencies.store
    main_cloud_router = dependencies.main_cloud_router
    if main_cloud_router.active_instance_id is None:
        return
    backend = main_cloud_router.for_owner(user)
    if not backend.is_initialized and backend.is_configured:
        await backend.ensure_initialized()
    if not backend.is_initialized:
        return

    # Phase 4 (cloud_collaboration_model.md §9): if the thread
    # already has any mount with a working webdav_url — project,
    # project_default, or repo — the legacy session folder would
    # be a redundant second sync target. Skip provisioning it.
    # The gate is observable-state: failed mount resolution
    # leaves no usable row, the legacy folder is still
    # provisioned as fallback, the thread never ends up with
    # zero cloud surfaces.
    try:
        existing_mounts = await store.list_thread_mounts(thread_id)
    except Exception as e:
        existing_mounts = []
        logger.warning(
            "Thread %s: failed to read thread_mounts before session "
            "folder provisioning (%s); proceeding with legacy folder.",
            thread_id,
            e,
        )
    if dependencies.should_skip_session_folder(existing_mounts):
        logger.info(
            "Thread %s: skipping legacy session folder — at least "
            "one mount with a working webdav_url is observable.",
            thread_id,
        )
        return

    try:
        session_handle = await backend.ensure_session_folder(session_id=thread_id[:8])
        share_handle = None
        # ensure_user (not just resolve_user_identity) so we synchronously
        # provision the cloud user record here — otherwise we race the
        # fire-and-forget JIT task from auth.get_current_user and the
        # share gets skipped on a user's very first session.
        resolved_user_id = await backend.ensure_user(
            sub=user.get("keycloak_sub") or "",
            issuer=getattr(backend, "_keycloak_issuer", "") or "",
            email=user.get("email"),
            display_name=user.get("display_name"),
            preferred_username=user.get("preferred_username"),
        )
        if resolved_user_id:
            share_handle = await backend.share_session_folder(
                session_handle, resolved_user_id
            )
        await store.update_thread_main_cloud(
            thread_id,
            backend_id=backend.backend_id,
            backend_instance_id=str(backend.backend_instance_id),
            session_handle=session_handle.to_db(),
            share_handle=share_handle.to_db() if share_handle else None,
        )
    except Exception as e:
        logger.warning(
            "Failed to provision main-cloud session folder for thread %s: %s",
            thread_id,
            e,
        )


async def _assign_pool_agent(
    tid: str,
    co: dict,
    pids: list,
    _ds_ids: list[str] | None,
    cfg_name: str | None = None,
    *,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    idle_agent = await dependencies.find_idle_persistent_agent()
    if idle_agent:
        # send_session_attach re-fetches the committed thread,
        # reauthorizes its current selection, and resolves secrets.
        # Do not pre-resolve a payload that can go stale while this
        # background task waits for a pool agent.
        await dependencies.send_session_attach(
            idle_agent,
            tid,
            co,
            pids,
            datasources=None,
            config_name=cfg_name,
        )
    else:
        logger.warning(
            "Thread %s: no idle agents in pool. "
            "Increase AGENT_REPLICAS or wait for a session to end.",
            tid,
        )


def schedule_thread_agent(
    plan: ThreadCreationPlan,
    thread_id: str,
    user: dict[str, Any],
    created_runtime_authority: Any,
    *,
    dependencies: ThreadAdmissionDependencies,
) -> None:
    """Provision the agent pod / assign one from the pool.

    Fires AFTER Gitea setup so the agent's workspace-readiness poll sees
    ``git_remote_url``. Priority: unified provisioner (K8s) → Docker Compose
    pool → manual.
    """
    use_k8s_agent = dependencies.agent_provisioner.is_available and (
        dependencies.agent_provisioner.in_cluster
        or not dependencies.docker_provisioner.is_available
    )
    if plan.execution_lane == "stateless":
        logger.info(
            "Thread %s: admitted to stateless session pool "
            "(workspace_backend=%s); dedicated agent provisioning skipped",
            thread_id,
            plan.thread_backend,
        )
    elif (
        plan.explicit_officer_commission
        and dependencies.persistent_provisioner.is_available
    ):
        # The interim Officer lifecycle owner deliberately manages only
        # finalizer-protected dedicated persistent Pods.  Sending an
        # explicit Post commission through ``provision_or_assign`` may
        # bind a generic warm-pool Pod instead; the Officer can run, but
        # the lifecycle scanner cannot observe/recycle that shape and
        # therefore installs a permanent ``unsupported_pod_authority``
        # hold on its next pass.  Commission directly onto the substrate
        # whose exact Pod/PVC UID and provision attempt the recycler owns.
        # Ordinary pinned sessions retain the warm-pool fast path below.
        asyncio.create_task(
            dependencies.provision_commissioned_officer(
                thread_id,
                user_id=str(user["id"]),
                config_name=plan.config_name,
                runtime_authority=created_runtime_authority,
            ),
            name=f"commission-officer-pod-{thread_id[:8]}",
        )
    elif use_k8s_agent:
        # Kubernetes mode: create agent pod on demand, with pool fallback
        effective_config = plan.config_name

        asyncio.create_task(
            dependencies.provision_or_assign(
                str(user["id"]),
                thread_id,
                effective_config,
                plan.config_override,
                plan.effective_project_ids,
                plan.selected_datasource_ids,
                runtime_generation=created_runtime_authority.generation,
            )
        )
    elif dependencies.docker_provisioner.is_available:
        # Docker Compose mode: find an idle pool agent and attach the thread
        asyncio.create_task(
            _assign_pool_agent(
                thread_id,
                plan.config_override,
                plan.effective_project_ids,
                plan.selected_datasource_ids,
                plan.config_name,
                dependencies=dependencies,
            )
        )
    else:
        logger.warning(
            "Thread %s: agent pod not provisioned — "
            "no provisioner available. Start the agent manually: "
            "python -m agent --mode persistent --thread-id %s",
            thread_id,
            thread_id,
        )


async def create_thread(
    request_body: ThreadCreateRequest,
    request: Request,
    *,
    dependencies: ThreadAdmissionDependencies,
    user: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new persistent thread with a concrete resolved expert.

    ``user`` is the already-approved principal when the route resolved it. The
    route has to run the gate in its own body — ``scripts/check_endpoint_auth.py``
    reads the audited gate out of the declaration, and a handler that only
    delegates is reported ``unscoped`` — so passing the result down is what
    keeps this from becoming a second identical approval read.
    """
    await dependencies.enforce_readiness_gate()
    try:
        if user is None:
            user = await dependencies.require_approved_user(request, dependencies.store)

        plan = await resolve_thread_creation_plan(
            request_body, user, dependencies=dependencies
        )
        thread_id, created_runtime_authority = await commit_thread_creation(
            plan, request_body, user, dependencies=dependencies
        )

        await provision_thread_workspace(plan, thread_id, dependencies=dependencies)

        # Run Gitea + Nextcloud setup in parallel, and AWAIT both before
        # assigning an agent. The workspace container is already provisioning
        # in the background above. If we fired the agent-attach first, the
        # agent could see `status=ready` on the workspace before setup_gitea
        # had written `git_remote_url`, so WorkspaceManager would init a
        # local-only repo with no origin — commits would accumulate but
        # never push. Blocking on gather here is cheap (Gitea create_repo is
        # ~50ms) and makes the workspace→remote wiring race-free.
        await asyncio.gather(
            setup_thread_gitea(
                thread_id,
                user,
                primary_project_id=plan.primary_project_id,
                lite_session=plan.lite_session,
                dependencies=dependencies,
            ),
            setup_thread_main_cloud(thread_id, user, dependencies=dependencies),
        )

        schedule_thread_agent(
            plan,
            thread_id,
            user,
            created_runtime_authority,
            dependencies=dependencies,
        )

        response: dict[str, Any] = {"thread_id": thread_id, "status": "created"}
        if plan.ignored_override_keys:
            # Warn phase: the caller learns which of its nested keys the
            # create rebuild did not carry. Additive — clients that do not
            # read it are unaffected; the Strict phase turns this into a 400.
            response["ignored_config_keys"] = plan.ignored_override_keys
        return response
    except DatasourceMaterializationAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Work owner is no longer authorized",
        ) from exc
    except DatasourcePolicyConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail="Connector policy changed while creating work; retry the request",
        ) from exc
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def list_threads(
    request: Request,
    project_id: str | None = None,
    status: str | None = None,
    *,
    dependencies: ThreadAdmissionDependencies,
    user: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """List persistent threads for the authenticated user."""
    try:
        if user is None:
            user = await dependencies.require_approved_user(request, dependencies.store)
        threads = await dependencies.store.list_threads(
            user_id=str(user["id"]),
            project_id=project_id,
            status=status,
        )
        # Phase 2: default-project threads have no legacy session folder, so the
        # cloud-button URL comes from the project_default mount row. Fetch every
        # thread's mounts in one query (was a per-thread N+1).
        mounts_by_thread = await dependencies.store.list_thread_mounts_bulk(
            [str(t["id"]) for t in threads]
        )
        for t in threads:
            t["cloud_session_url"] = dependencies.resolve_cloud_session_url(
                t, mounts_by_thread.get(str(t["id"]), [])
            )
        if getattr(dependencies.store, "supports_vm_creation_retry", False):
            from orchestrator.services.vm_creation_owner_view import thread_creation_views

            progress = await thread_creation_views(
                dependencies.store, [str(t["id"]) for t in threads],
                viewer_user_id=str(user["id"]), admin=user.get("is_admin") is True,
            )
            for t in threads:
                t["vm_creation"] = progress.get(str(t["id"]))
        return {"threads": [dependencies.redact_thread_metadata(t) for t in threads]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "ThreadAdmissionDependencies",
    "ThreadAdmissionStore",
    "ThreadCreationPlan",
    "build_session_config_override",
    "commit_thread_creation",
    "create_thread",
    "list_threads",
    "provision_thread_workspace",
    "resolve_thread_creation_plan",
    "schedule_thread_agent",
    "select_thread_datasources",
    "setup_thread_gitea",
    "setup_thread_main_cloud",
]
