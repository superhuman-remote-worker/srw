"""Canonical execution snapshots for the reference harness's existing callers.

Only this explicitly selected adapter reads SRW settings. Native admissions pass
an already authorized manifest directly to ``capture_execution`` and never enter
the loader, redactor or model/tool policy below. All admission writes use the
caller's connection; delivery rechecks authority and adds transient bindings to a
copy of the stored configuration.
"""

from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.manifest_store import ManifestStore, decoded
from shared.manifests.resolution import content_revision


SRW_ADAPTER = "srw/v1"
SNAPSHOT_FORMAT = "srw/resolved-config-v1"


class ExecutionGrantDenied(HTTPException):
    """The rendered policy exceeds the runner's grants.

    Wire-identical to the plain 422 every caller already handles; the
    violations stay structured for callers that must name the missing grants
    (job admission's default-expert preview).
    """

    def __init__(self, violations: list[str]):
        from orchestrator.services.grant_enforcement import grant_violations_detail

        super().__init__(422, grant_violations_detail(violations))
        self.violations = list(violations)


def object_value(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Execution configuration must be an object")
    return deepcopy(value)


async def read_execution(store: Any, work_kind: str, work_id: str) -> dict | None:
    """Historical rows have no snapshot; malformed current rows fail closed."""
    row = await store.fetchrow(
        "SELECT * FROM srw_execution_specs WHERE work_kind=$1 AND work_id=$2",
        work_kind,
        UUID(str(work_id)),
    )
    if row is None:
        return None
    result = decoded(row)
    if not result:
        return None
    return result


def srw_snapshot_config(snapshot: dict) -> tuple[dict, dict]:
    """Read an immutable rendered SRW payload without resolving live experts."""
    if snapshot.get("harness_adapter") != SRW_ADAPTER:
        raise HTTPException(
            409, "This execution requires its selected harness runtime."
        )
    resolved = object_value(snapshot["resolved"])
    runtime = resolved["spec"]["execution"]["expert"]["inline"]["runtime"]
    private = object_value(runtime.get("config"))
    if private.get("format") != SNAPSHOT_FORMAT:
        raise HTTPException(409, "The SRW execution snapshot format is unsupported.")
    blob = object_value(private.get("resolved"))
    policy = object_value(private.get("policy"))
    if not isinstance(blob.get("agent"), dict) or not policy:
        raise HTTPException(409, "The SRW execution snapshot is incomplete.")
    return blob, policy


def apply_srw_delivery_bindings(
    blob: dict,
    policy: dict,
    override: dict | None,
    *,
    grant_strip: Any = None,
) -> tuple[dict, dict]:
    """Add current workspace/tool bindings to the reference harness copy only.

    The incoming fragment is produced by the existing authorized workspace and
    datasource builders. Model, prompt and other private settings remain those
    admitted in the snapshot. Credential injection happens after the policy check.
    """
    from shared.runtime.core.loader import deep_merge

    transient = {}
    for key in ("workspace", "tools"):
        if isinstance((override or {}).get(key), dict):
            transient[key] = deepcopy(override[key])
    # Workspace delivery selects the sudo gate too: VM commands reach the
    # guest's gate, while a denied sandbox upgrade must remain blocked. Other
    # private shell settings still come exclusively from the admitted snapshot.
    shell = (override or {}).get("shell")
    if isinstance(shell, dict):
        sudo = {
            key: deepcopy(shell[key])
            for key in ("sudo_action", "sudo_block_message")
            if key in shell
        }
        if sudo:
            transient["shell"] = sudo
    result = deepcopy(blob)
    result["agent"] = deep_merge(result["agent"], transient)
    checked = deep_merge(deepcopy(policy), transient)
    if grant_strip is not None:
        checked = grant_strip(checked)
        result["agent"] = grant_strip(result["agent"])
    return result, checked


def validate_srw_authored_fragment(value: Any) -> dict:
    """Apply the reference harness's write gates before private resolution."""
    from orchestrator.services.session_tool_policy import with_validated_tool_overrides
    from shared.runtime.core.expert_resolution import canonical_key, hard_deny_scan
    from shared.runtime.core.subagent_roster import (
        RosterResolutionError,
        validate_roster_fragment,
    )

    try:
        fragment = object_value(value)
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            422, "SRW private configuration must be an object."
        ) from exc
    offending = set(hard_deny_scan(fragment))
    reserved = {
        "connections",
        "runtimeactor",
        "recipient",
        "workspaceruntime",
        "workspacebinding",
        "workspacegeneration",
        "workspaceruntimeincarnation",
        "workspaceownerid",
        "workspaceownerkind",
        "workspacesshhostkeyfingerprint",
    }

    def scan(item: Any, path: tuple[str, ...] = ()) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                current = (*path, str(key))
                normalized = canonical_key(str(key))
                if normalized in reserved or (
                    path
                    and canonical_key(path[-1]) == "workspace"
                    and normalized in {"remote", "mounts"}
                ):
                    offending.add(".".join(current))
                scan(child, current)
        elif isinstance(item, list):
            for index, child in enumerate(item):
                scan(child, (*path, str(index)))

    scan(fragment)
    if offending:
        raise HTTPException(
            422,
            "SRW private config may not set credential or runtime authority sections: "
            + ", ".join(sorted(offending)),
        )
    validated = with_validated_tool_overrides(fragment) or {}
    subagents = validated.get("subagents")
    if subagents is not None:
        try:
            validate_roster_fragment(subagents)
        except RosterResolutionError as exc:
            raise HTTPException(422, str(exc)) from exc
        if isinstance(subagents, dict) and "roster" in subagents:
            validated["subagents"] = {
                **subagents,
                "roster": {
                    name: validate_srw_authored_fragment(entry)
                    for name, entry in (subagents["roster"] or {}).items()
                },
            }
    return validated


async def prepare_srw_snapshot(
    db: Any,
    *,
    work_kind: str,
    work_id: str,
    owner_id: str | None,
    project_ids: list[str],
    config_name: str,
    expert_id: str | None,
    config_override: dict | None,
    description: str,
    datasource_ids: list[str],
    policy_revisions: dict[str, int],
    runner_kind: str = "user",
    expert_row: dict | None = None,
    workspace_selection: dict | None = None,
) -> dict:
    """Render the complete SRW configuration while the insertion is locked.

    The caller binds ``db`` to its active transaction before entering. Account
    defaults, the selected Expert and roster references are captured once. Live
    connector credentials and workspace endpoints are deliberately absent.
    """
    from orchestrator.services import session_config_resolution as session_config
    from orchestrator.services.config_resolver import resolve_config
    from orchestrator.services.dispatch_credentials import (
        DispatchCredentialDependencies,
        seed_registry_model_overrides,
    )
    from orchestrator.services.grant_enforcement import (
        resolve_runner_grants,
        user_experts_enabled,
    )
    from orchestrator.services.manifest_experts import installed_srw_image
    from orchestrator.services.manifest_projects import project_expert_for_execution
    from orchestrator.services.config_overrides import validated_config_name
    from shared.runtime.core.capability_grants import evaluate
    from shared.runtime.core.expert_resolution import (
        validate_expert_persona_placeholders,
    )
    from shared.runtime.core.model_registry import resolve_model
    from shared.runtime.core.skill_resolution import filter_bound_skills

    if workspace_selection is not None:
        from orchestrator.services.manifest_workspace_selection import (
            verify_workspace_selection,
        )

        await ManifestStore(db).lock_catalog()
        await verify_workspace_selection(db, workspace_selection, owner_id)

    if expert_row is not None and expert_id is not None:
        raise ValueError(
            "Supply an authorized Expert definition or its identity, not both"
        )
    role = "session" if work_kind == "Session" else "worker"
    expert = deepcopy(expert_row) if expert_row is not None else None
    if expert_row is None and len(project_ids) == 1:
        expert = await project_expert_for_execution(
            db, project_ids[0], expert_id=expert_id, role=role, config_name=config_name
        )
    if expert is None and expert_id:
        expert = await db.get_expert_by_id(str(expert_id))
    if expert is None and expert_row is None and not expert_id:
        from orchestrator.services.manifest_experts import bundled_expert_for_execution

        expert = await bundled_expert_for_execution(db, config_name)
    if expert_id and expert is None:
        raise HTTPException(409, "The selected Expert is no longer available.")
    from orchestrator.services.manifest_runtime_ownership import (
        require_srw_expert_configuration,
    )

    trusted_image = getattr(db, "manifest_runtime_image", None) or installed_srw_image()
    require_srw_expert_configuration(
        expert, interactive=role == "session", trusted_image=trusted_image
    )
    if expert is not None:
        expert = deepcopy(expert)
        expert["config"] = validate_srw_authored_fragment(expert.get("config"))
        private_layers = expert.get("harness_config_layers", [])
        if not isinstance(private_layers, list) or any(
            not isinstance(layer, dict) for layer in private_layers
        ):
            raise HTTPException(422, "SRW private layers must be an array of objects.")
        expert["harness_config_layers"] = [
            validate_srw_authored_fragment(layer) for layer in private_layers
        ]
        try:
            validate_expert_persona_placeholders(object_value(expert.get("prompts")))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
    config_name = (expert or {}).get("harness_config_name") or config_name
    config_name = validated_config_name(config_name) or (
        "session_base" if role == "session" else "worker_base"
    )
    deps = SimpleNamespace(store=db)
    defaults = await (
        session_config.resolve_session_account_defaults(owner_id, dependencies=deps)
        if role == "session"
        else session_config.resolve_default_models(owner_id, dependencies=deps)
    )
    project_override = None
    if (
        role == "session"
        and expert_id
        and len(project_ids) == 1
        and not (expert or {}).get("project_composed")
    ):
        link = await db.get_project_expert_link(
            project_id=project_ids[0], expert_id=str(expert_id)
        )
        if link:
            project_override = object_value(link.get("config_override"))
    requested = await seed_registry_model_overrides(
        deepcopy(config_override),
        user_id=owner_id,
        dependencies=DispatchCredentialDependencies(
            store=db, logger=None, resolve_model=resolve_model
        ),
    )
    private_layers = (expert or {}).get("harness_config_layers") or []
    # A resource can have been written through native apply, outside the old
    # expert-save visibility gate. Every private roster reference is authorized
    # against the runner before it becomes part of an execution snapshot.
    roster_overrides = [*private_layers, project_override, requested]
    if expert is not None:
        roster_overrides.insert(0, object_value(expert.get("config")))
    refs = await session_config.prefetch_roster_refs(
        expert_row=None,
        overrides=roster_overrides,
        user_id=owner_id,
        project_ids=project_ids,
        dependencies=deps,
    )
    for key, source in list(refs.items()):
        # Native child resources have not passed the compatibility save gate.
        # Never normalize a generic child's private configuration as SRW.
        if not source or source.get("harness_adapter", SRW_ADAPTER) != SRW_ADAPTER:
            continue
        require_srw_expert_configuration(source, trusted_image=trusted_image)
        source = deepcopy(source)
        source["config"] = validate_srw_authored_fragment(source.get("config"))
        layers = source.get("harness_config_layers", [])
        if not isinstance(layers, list) or any(
            not isinstance(layer, dict) for layer in layers
        ):
            raise HTTPException(422, "SRW private layers must be an array of objects.")
        source["harness_config_layers"] = [
            validate_srw_authored_fragment(layer) for layer in layers
        ]
        try:
            validate_expert_persona_placeholders(object_value(source.get("prompts")))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        refs[key] = source
    skills_provider = getattr(db, "manifest_skills_provider", None)
    skills = (
        await skills_provider(owner_id, project_ids)
        if callable(skills_provider)
        else None
    )
    capture: dict[str, Any] = {}
    blob = resolve_config(
        base_config_name=config_name,
        base_defaults=defaults,
        expert_row=expert,
        project_overrides=project_override,
        request_override=requested,
        expert_type=role,
        capture=capture,
        skills=skills,
        db_refs=refs,
    )
    filter_bound_skills(blob)
    policy = capture["merged_fragment"]
    if await user_experts_enabled(dependencies=deps):
        grants = await resolve_runner_grants(
            runner_user_id=owner_id,
            project_ids=project_ids,
            runner_kind=runner_kind,
            dependencies=deps,
        )
        if grants is not None:
            violations = evaluate(policy, grants)
            if violations:
                raise ExecutionGrantDenied(violations)
    dependencies = []
    if (expert or {}).get("project_dependency"):
        dependencies.append(deepcopy(expert["project_dependency"]))
    for referenced in [expert, *refs.values()]:
        if referenced and referenced.get("manifest_uid"):
            dependency = {
                "uid": referenced["manifest_uid"],
                "revision": referenced["manifest_revision"],
            }
            if dependency not in dependencies:
                dependencies.append(dependency)
    prepared = rendered_srw_snapshot(
        blob,
        policy,
        work_kind=work_kind,
        work_id=work_id,
        owner_id=owner_id,
        project_ids=project_ids,
        config_name=config_name,
        description=description,
        datasource_ids=datasource_ids,
        policy_revisions=policy_revisions,
        image=trusted_image,
        dependencies=dependencies,
        asset_name=(expert or {}).get("harness_asset_name"),
    )
    if workspace_selection is not None:
        from orchestrator.services.manifest_workspace_selection import (
            srw_workspace_config,
        )

        expected = srw_workspace_config(
            workspace_selection["resolved"],
            instance_recipe=workspace_selection.get("instance_recipe"),
        )
        actual = policy.get("workspace") or {}
        if any(actual.get(key) != value for key, value in expected.items()):
            raise HTTPException(409, "Workspace assignment changed during admission.")
        for key in ("document", "resolved"):
            prepared[key]["spec"]["execution"]["workspace"] = deepcopy(
                workspace_selection[key]
            )
        for dependency in workspace_selection["dependencies"]:
            if dependency not in prepared["dependencies"]:
                prepared["dependencies"].append(deepcopy(dependency))
        prepared["revision"] = content_revision(prepared["resolved"]["spec"])
    return prepared


def rendered_srw_snapshot(
    blob: dict,
    policy: dict,
    *,
    work_kind: str,
    work_id: str,
    owner_id: str | None,
    project_ids: list[str],
    config_name: str,
    description: str,
    datasource_ids: list[str],
    policy_revisions: dict[str, int],
    image: str,
    dependencies: list[dict],
    asset_name: str | None = None,
) -> dict:
    """Wrap rendered private configuration, including exact historical imports."""
    from orchestrator.security.access import redact_config_override

    blob = redact_config_override(blob)
    policy = redact_config_override(policy)
    for value in (blob["agent"], policy):
        workspace = value.get("workspace")
        if isinstance(workspace, dict):
            workspace.pop("remote", None)
            workspace.pop("mounts", None)
    backend = (blob["agent"].get("workspace") or {}).get("backend", "sandbox")
    backend = {"remote": "sandbox", "container": "sandbox"}.get(backend, backend)
    workspace = (
        None
        if backend == "none"
        else {
            "template": {"inline": {"backend": backend}},
        }
    )
    runtime = {
        "image": image,
        "adapter": SRW_ADAPTER,
        "pullPolicy": "IfNotPresent",
        "config": {
            "format": SNAPSHOT_FORMAT,
            "config_name": config_name,
            "resolved": blob,
            "policy": policy,
        },
    }
    if asset_name is not None:
        runtime["config"]["asset_name"] = asset_name
    scope = (
        {"kind": "Account", "name": owner_id}
        if owner_id
        else {"kind": "Catalog", "name": "shared"}
    )
    if len(project_ids) == 1:
        scope = {"kind": "Project", "name": project_ids[0]}
    connectors = {
        f"source-{source_id}": {
            "inline": {
                "driver": "srw.datasource/v1",
                "config": {
                    "datasourceId": source_id,
                    "policyRevision": int(policy_revisions.get(source_id, 0)),
                },
            }
        }
        for source_id in datasource_ids
    }
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {
            "name": f"{work_kind.lower()}-{work_id}",
            "scope": scope,
            "annotations": {
                "srw.io/work-kind": work_kind,
                "srw.io/configuration-state": "Resolved",
            },
        },
        "spec": {
            "task": {"text": description or "Interactive session"},
            "execution": {
                "expert": {"inline": {"runtime": runtime}},
                "workspace": workspace,
                "connectors": connectors,
            },
        },
    }
    return {
        "document": document,
        "resolved": deepcopy(document),
        "revision": content_revision(document["spec"]),
        "dependencies": dependencies,
        "harness_adapter": SRW_ADAPTER,
    }


async def prepare_srw_session_patch(
    db: Any,
    current: dict,
    thread: dict,
    metadata: dict,
    project_ids: list[str],
    config_override: dict,
) -> tuple[dict, dict]:
    """Patch admitted settings without consulting changed configuration sources."""
    from orchestrator.services.grant_enforcement import (
        grant_violations_detail,
        resolve_runner_grants,
        user_experts_enabled,
    )
    from shared.runtime.core.capability_grants import evaluate
    from shared.runtime.core.loader import resolve_config_path
    from shared.runtime.core.session_config_patch import patch_frozen_session

    blob, policy = srw_snapshot_config(current)
    runtime = current["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]
    private = runtime["config"]
    _, deployment_dir = resolve_config_path(
        private.get("asset_name") or private["config_name"]
    )
    updated, policy, delivery_override = patch_frozen_session(
        blob, policy, config_override, deployment_dir=deployment_dir
    )
    # Ordered controls are an independent authority; the saved policy must
    # check the same current values that the next attach will deliver.
    interactive = {"permission_mode": thread.get("permission_mode") or "supervised"}
    if thread.get("narration_mode") is not None:
        interactive["narration_mode"] = thread["narration_mode"]
    for fragment in (updated["agent"], policy):
        fragment["interactive"] = {**(fragment.get("interactive") or {}), **interactive}
    owner_id = str(thread["user_id"]) if thread.get("user_id") else None
    deps = SimpleNamespace(store=db)
    if await user_experts_enabled(dependencies=deps):
        grants = await resolve_runner_grants(
            runner_user_id=owner_id, project_ids=project_ids, dependencies=deps
        )
        if grants is not None and (violations := evaluate(policy, grants)):
            raise HTTPException(422, grant_violations_detail(violations))
    selection = object_value(metadata.get("datasource_selection"))
    prepared = rendered_srw_snapshot(
        updated,
        policy,
        work_kind="Session",
        work_id=str(thread["id"]),
        owner_id=owner_id,
        project_ids=project_ids,
        config_name=private["config_name"],
        asset_name=private.get("asset_name"),
        description=current["resolved"]["spec"]["task"]["text"],
        datasource_ids=metadata.get("datasource_ids") or [],
        policy_revisions=selection.get("policy_revisions") or {},
        image=runtime["image"],
        dependencies=deepcopy(current["dependencies"]),
    )
    old_workspace = current["resolved"]["spec"]["execution"]["workspace"]
    new_workspace = prepared["resolved"]["spec"]["execution"]["workspace"]

    def backend_of(selection):
        return (
            (selection or {})
            .get("template", {})
            .get("inline", {})
            .get("backend", "none")
        )

    # Checked on every patch, whatever the backend transition: only admission
    # validates these blocks, so a tier change may bind a bare backend only.
    _, old_policy = srw_snapshot_config(current)
    captured_messages = {
        "vm": "Session settings cannot change the captured VM image or "
        "resources; create a new session with the selected workspace.",
        "sandbox": "Session settings cannot change the captured container "
        "image or resources; create a new session with the selected workspace.",
    }
    for captured, message in captured_messages.items():
        if (policy.get("workspace") or {}).get(captured) != (
            old_policy.get("workspace") or {}
        ).get(captured):
            raise HTTPException(422, message)
    if backend_of(old_workspace) == backend_of(new_workspace):
        for key in ("document", "resolved"):
            prepared[key]["spec"]["execution"]["workspace"] = deepcopy(
                current[key]["spec"]["execution"]["workspace"]
            )
        prepared["revision"] = content_revision(prepared["resolved"]["spec"])
    prepared["expected_generation"] = current["generation"]
    return prepared, delivery_override


async def capture_execution(
    db: Any,
    conn: Any,
    *,
    work_kind: str,
    work_id: str,
    owner_id: str | None,
    project_ids: list[str],
    execution_manifest: dict | None = None,
    replace_session: bool = False,
    **srw_inputs: Any,
) -> dict:
    """Capture one new execution on the exact admission transaction."""
    async with db.using_connection(conn):
        store = ManifestStore(db)
        await store.lock_catalog()
        prepared = (
            deepcopy(execution_manifest)
            if execution_manifest is not None
            else await prepare_srw_snapshot(
                db,
                work_kind=work_kind,
                work_id=work_id,
                owner_id=owner_id,
                project_ids=project_ids,
                **srw_inputs,
            )
        )
        if replace_session:
            if work_kind != "Session":
                raise ValueError("Only session specifications may be updated")
            return await store.update_session_execution(
                work_id=work_id,
                owner_id=owner_id,
                project_ids=project_ids,
                conn=conn,
                **prepared,
            )
        snapshot = await store.freeze_execution(
            work_kind=work_kind,
            work_id=work_id,
            owner_id=owner_id,
            project_ids=project_ids,
            conn=conn,
            **prepared,
        )

        from orchestrator.services.retained_vm_workspaces import reserve

        await reserve(db, snapshot)
        return snapshot
