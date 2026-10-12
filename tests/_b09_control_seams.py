"""Direct test adapters for R1.B09 router and operation owners.

These helpers preserve the former positional call shapes used by older tests
without adding compatibility wrappers back to the application module. Each
call resolves the real application dependency factory at invocation time, so
patches of app-owned collaborators still exercise the extracted boundary.
"""

from __future__ import annotations

from typing import Any

import orchestrator.main as main
from orchestrator.routers import (
    job_assignment,
    job_controls,
    job_lifecycle,
    thread_lifecycle,
)
from orchestrator.application import controls as controls_composition
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.services import job_start_bundle as job_start_bundle_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import thread_config_update as thread_config_update_module
from orchestrator.services import (
    thread_datasource_authorization as thread_datasource_authorization_module,
)
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)
from orchestrator.services import thread_resume as thread_resume_module
from orchestrator.services import thread_retirement as thread_retirement_module
from orchestrator.services import (
    thread_workspace_delivery as thread_workspace_delivery_module,
)


async def create_job(request: Any, body: Any) -> Any:
    return await job_lifecycle.create_job(
        request,
        body,
        dependencies=jobs_composition.job_lifecycle_route_dependencies(
            main.app.state.resources
        ),
    )


async def assign_job_to_agent(request: Any, job_id: str, agent_id: str) -> Any:
    return await job_assignment.assign_job_to_agent(
        request,
        job_id,
        agent_id,
        dependencies=controls_composition.job_assignment_dependencies(
            main.app.state.resources
        ),
    )


async def agent_get_thread_workspace_locked(
    thread_id: str, **kwargs: Any
) -> dict[str, Any]:
    return await thread_workspace_delivery_module.agent_get_thread_workspace_locked(
        thread_id,
        dependencies=preparation_composition.thread_workspace_delivery_dependencies(
            main.app.state.resources
        ),
        **kwargs,
    )


async def subjob_merge(request: Any, job_id: str) -> Any:
    return await job_lifecycle.subjob_merge(
        request,
        job_id,
        dependencies=jobs_composition.job_lifecycle_route_dependencies(
            main.app.state.resources
        ),
    )


async def delete_job(request: Any, job_id: str) -> Any:
    return await job_lifecycle.delete_job(
        request,
        job_id,
        dependencies=controls_composition.job_mutation_route_dependencies(
            main.app.state.resources
        ),
    )


async def cancel_job(request: Any, job_id: str) -> Any:
    return await job_lifecycle.cancel_job(
        request,
        job_id,
        dependencies=controls_composition.job_mutation_route_dependencies(
            main.app.state.resources
        ),
    )


async def pause_job(request: Any, job_id: str) -> Any:
    return await job_lifecycle.pause_job(
        request,
        job_id,
        dependencies=controls_composition.job_mutation_route_dependencies(
            main.app.state.resources
        ),
    )


async def agent_release_job(
    request: Any,
    job_id: str,
    agent_id: str | None = None,
    lease_token: int | None = None,
) -> Any:
    return await job_lifecycle.agent_release_job(
        request,
        job_id,
        agent_id,
        lease_token,
        dependencies=controls_composition.job_mutation_route_dependencies(
            main.app.state.resources
        ),
    )


async def create_vm(request: Any, body: Any) -> Any:
    return await job_controls.create_vm(
        request,
        body,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def list_vms(request: Any) -> Any:
    return await job_controls.list_vms(
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def get_vm_status(request: Any, job_id: str, live: bool = False) -> Any:
    return await job_controls.get_vm_status(
        request,
        job_id,
        live,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def delete_vm(request: Any, job_id: str) -> Any:
    return await job_controls.delete_vm(
        request,
        job_id,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def sudo_sse_events(request: Any) -> Any:
    return await job_controls.sudo_sse_events(
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def list_sudo_requests(request: Any, **kwargs: Any) -> Any:
    return await job_controls.list_sudo_requests(
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
        **kwargs,
    )


async def get_sudo_request(request: Any, request_id: str) -> Any:
    return await job_controls.get_sudo_request(
        request,
        request_id,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def approve_sudo_request(
    request_id: str, body: Any = None, request: Any = None
) -> Any:
    return await job_controls.approve_sudo_request(
        request_id,
        body,
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def deny_sudo_request(request_id: str, body: Any, request: Any) -> Any:
    return await job_controls.deny_sudo_request(
        request_id,
        body,
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def approve_sudo_vm_upgrade(
    request_id: str, body: Any = None, request: Any = None
) -> Any:
    return await job_controls.approve_sudo_vm_upgrade(
        request_id,
        body,
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def resume_sudo_without_vm(
    request_id: str, body: Any = None, request: Any = None
) -> Any:
    return await job_controls.resume_sudo_without_vm(
        request_id,
        body,
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def list_sudo_rules(request: Any) -> Any:
    return await job_controls.list_sudo_rules(
        request,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def create_sudo_rule(request: Any, body: Any) -> Any:
    return await job_controls.create_sudo_rule(
        request,
        body,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def delete_sudo_rule(request: Any, rule_id: str) -> Any:
    return await job_controls.delete_sudo_rule(
        request,
        rule_id,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def resume_job(req: Any, job_id: str, body: Any = None) -> Any:
    return await job_controls.resume_job(
        req,
        job_id,
        body,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def approve_job(req: Any, job_id: str, body: Any = None) -> Any:
    return await job_controls.approve_job(
        req,
        job_id,
        body,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def upgrade_job_to_vm(request: Any, job_id: str) -> Any:
    return await job_controls.upgrade_job_to_vm(
        request,
        job_id,
        dependencies=controls_composition.job_control_route_dependencies(
            main.app.state.resources
        ),
    )


async def end_thread(
    thread_id: str,
    request: Any,
    permanent: bool = False,
    force: bool = False,
) -> Any:
    return await thread_lifecycle.end_thread(
        thread_id,
        request,
        permanent,
        force,
        dependencies=controls_composition.thread_lifecycle_dependencies(
            main.app.state.resources
        ),
    )


async def resume_thread(thread_id: str, request: Any, body: Any = None) -> Any:
    return await thread_lifecycle.resume_thread(
        thread_id,
        request,
        body,
        dependencies=controls_composition.thread_lifecycle_dependencies(
            main.app.state.resources
        ),
    )


async def rewind_thread_detached(thread_id: str, request: Any, body: Any) -> Any:
    return await thread_lifecycle.rewind_thread_detached(
        thread_id,
        request,
        body,
        dependencies=controls_composition.thread_lifecycle_dependencies(
            main.app.state.resources
        ),
    )


async def dispatch_job_to_agent(job: dict[str, Any], agent: dict[str, Any]) -> bool:
    return await controls_composition.job_delivery_operations(
        main.app.state.resources
    ).dispatch(job, agent)


async def resume_job_on_agent(job: dict[str, Any], agent: dict[str, Any]) -> bool:
    return await controls_composition.job_delivery_operations(
        main.app.state.resources
    ).resume(job, agent)


async def build_job_start_request(*args: Any, **kwargs: Any) -> Any:
    return await job_start_bundle_module.build_job_start_request(
        *args,
        **kwargs,
        dependencies=preparation_composition.job_start_bundle_dependencies(
            main.app.state.resources
        ),
    )


async def attest_pinned_k8s_job_workspace(*args: Any, **kwargs: Any) -> Any:
    return await job_workspace_authority_module.attest_pinned_k8s_job_workspace(
        *args,
        **kwargs,
        dependencies=preparation_composition.job_workspace_authority_dependencies(
            main.app.state.resources
        ),
    )


async def pinned_k8s_job_workspace_authority_is_current(
    *args: Any, **kwargs: Any
) -> Any:
    return await (
        job_workspace_authority_module.pinned_k8s_job_workspace_authority_is_current(
            *args,
            **kwargs,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
    )


def inject_matching_workspace_config(*args: Any, **kwargs: Any) -> Any:
    return job_workspace_runtime_module.inject_matching_workspace_config(
        *args,
        **kwargs,
        dependencies=preparation_composition.job_workspace_runtime_dependencies(
            main.app.state.resources
        ),
    )


def resume_missing_workspace(*args: Any, **kwargs: Any) -> Any:
    return job_workspace_runtime_module.resume_missing_workspace(
        *args,
        **kwargs,
        dependencies=preparation_composition.job_workspace_runtime_dependencies(
            main.app.state.resources
        ),
    )


async def classify_thread_project_ids(*args: Any, **kwargs: Any) -> Any:
    return await thread_project_authorization_module.classify_thread_project_ids(
        *args,
        **kwargs,
        dependencies=sessions_composition.thread_project_authorization_dependencies(
            main.app.state.resources
        ),
    )


async def authorize_thread_datasource_ids(*args: Any, **kwargs: Any) -> Any:
    return await thread_datasource_authorization_module.authorize_thread_datasource_ids(
        *args,
        **kwargs,
        dependencies=sessions_composition.thread_datasource_authorization_dependencies(
            main.app.state.resources
        ),
    )


async def revalidate_thread_datasource_selection(*args: Any, **kwargs: Any) -> Any:
    return await thread_datasource_authorization_module.revalidate_thread_datasource_selection(
        *args,
        **kwargs,
        dependencies=sessions_composition.thread_datasource_authorization_dependencies(
            main.app.state.resources
        ),
    )


async def apply_thread_config_update(*args: Any, **kwargs: Any) -> Any:
    return await thread_config_update_module.apply_thread_config_update(
        *args,
        **kwargs,
        dependencies=sessions_composition.thread_config_update_dependencies(
            main.app.state.resources
        ),
    )


async def initiate_pause(job: dict[str, Any]) -> None:
    await controls_composition.job_delivery_operations(
        main.app.state.resources
    ).initiate_pause(job)


async def resume_job_internal(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).resume_job_internal(*args, **kwargs)


async def approve_job_internal(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).approve_job_internal(*args, **kwargs)


async def upgrade_job_to_vm_internal(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).upgrade_job_to_vm_internal(*args, **kwargs)


async def apply_vm_upgrade_decision(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).apply_vm_upgrade_decision(*args, **kwargs)


async def resume_job_without_vm_internal(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).resume_job_without_vm_internal(*args, **kwargs)


async def internal_resume_job(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).internal_resume_job(*args, **kwargs)


def job_frozen_for_vm_upgrade(job: dict[str, Any] | None) -> bool:
    return controls_composition.job_control_operations(
        main.app.state.resources
    ).job_frozen_for_vm_upgrade(job)


async def unmerged_pr_gate_reason(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).unmerged_pr_gate_reason(*args, **kwargs)


async def capture_workspace_snapshot_for_freeze(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).capture_workspace_snapshot_for_freeze(*args, **kwargs)


async def fail_expired_vm_upgrade_jobs() -> int:
    return await controls_composition.job_control_operations(
        main.app.state.resources
    ).fail_expired_vm_upgrade_jobs()


async def cascade_pause_to_children(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_mutation_operations(
        main.app.state.resources
    ).cascade_pause_to_children(*args, **kwargs)


async def cascade_cancel_to_children(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_mutation_operations(
        main.app.state.resources
    ).cascade_cancel_to_children(*args, **kwargs)


async def wait_for_stateless_cancel_settle(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.job_mutation_operations(
        main.app.state.resources
    ).wait_for_stateless_cancel_settle(*args, **kwargs)


async def archive_and_cleanup_workspace(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).archive_and_cleanup_workspace(*args, **kwargs)


async def detach_agent_session(*args: Any, **kwargs: Any) -> Any:
    return await thread_retirement_module.detach_agent_session(
        *args,
        **kwargs,
        dependencies=controls_composition.thread_retirement_operations(
            main.app.state.resources
        ).dependencies,
    )


async def release_thread_resources(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).release_thread_resources(*args, **kwargs)


async def suspend_thread_resources(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).suspend_thread_resources(*args, **kwargs)


async def thread_turn_in_flight(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).thread_turn_in_flight(*args, **kwargs)


def stateless_retirement_marker(*args: Any, **kwargs: Any) -> Any:
    return controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).stateless_retirement_marker(*args, **kwargs)


async def reconcile_stateless_thread_retirement(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_retirement_operations(
        main.app.state.resources
    ).reconcile_stateless_thread_retirement(*args, **kwargs)


async def thread_config_drift(*args: Any, **kwargs: Any) -> Any:
    return await thread_resume_module.thread_config_drift(
        *args,
        **kwargs,
        dependencies=controls_composition.thread_resume_operations(
            main.app.state.resources
        ).dependencies,
    )


async def await_late_cloud_setup(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_resume_operations(
        main.app.state.resources
    ).await_late_cloud_setup(*args, **kwargs)


def register_late_cloud_setup(*args: Any, **kwargs: Any) -> Any:
    return controls_composition.thread_resume_operations(
        main.app.state.resources
    ).register_late_cloud_setup(*args, **kwargs)


async def resolve_background_push_workspace(*args: Any, **kwargs: Any) -> Any:
    return await controls_composition.thread_resume_operations(
        main.app.state.resources
    ).resolve_background_push_workspace(*args, **kwargs)


async def suspend_thread_resources_inner(*args: Any, **kwargs: Any) -> Any:
    return await thread_retirement_module.suspend_thread_resources_inner(
        *args,
        **kwargs,
        dependencies=controls_composition.thread_retirement_operations(
            main.app.state.resources
        ).dependencies,
    )
