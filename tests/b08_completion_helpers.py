"""Test adapters for the R1.B08 application composition boundary.

Owner-level unit tests construct dependency objects directly.  The older
integration-style suites use these helpers when they need the collaborators
assembled by ``orchestrator.main`` without restoring deleted main wrappers.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from orchestrator import main
from orchestrator.services import (
    completion_effects,
    curation_final_pass,
    job_freeze_notifications,
    job_completion,
    subjob_completion,
    subjob_output,
    verification_workflow,
)
from orchestrator.application import completion as completion_composition
from orchestrator.application import workflows as workflows_composition
from orchestrator.services import legacy_job_completion as legacy_job_completion_module


async def complete_job(*args: Any, **kwargs: Any) -> Any:
    return await job_completion.complete_job(
        *args,
        **kwargs,
        dependencies=completion_composition.job_completion_dependencies(
            main.app.state.resources
        ),
    )


async def complete_job_legacy(*args: Any, **kwargs: Any) -> Any:
    return await legacy_job_completion_module.complete_job_legacy(
        *args,
        **kwargs,
        dependencies=completion_composition.legacy_completion_dependencies(
            main.app.state.resources
        ),
    )


async def resolve_job_repo(*args: Any, **kwargs: Any) -> Any:
    return await subjob_output.resolve_job_repo(
        *args,
        **kwargs,
        dependencies=completion_composition.subjob_output_dependencies(
            main.app.state.resources
        ),
    )


async def graft_subjob_output(*args: Any, **kwargs: Any) -> Any:
    return await subjob_output.graft_subjob_output(
        *args,
        **kwargs,
        dependencies=completion_composition.subjob_output_dependencies(
            main.app.state.resources
        ),
    )


async def maybe_graft_completed_subjob(*args: Any, **kwargs: Any) -> Any:
    return await subjob_output.maybe_graft_completed_subjob(
        *args,
        **kwargs,
        dependencies=completion_composition.subjob_output_dependencies(
            main.app.state.resources
        ),
    )


async def spawn_scholar_subjob(*args: Any, **kwargs: Any) -> Any:
    return await subjob_completion.spawn_scholar_subjob(
        *args,
        **kwargs,
        dependencies=completion_composition.scholar_completion_dependencies(
            main.app.state.resources
        ),
    )


async def handle_scholar_completion(*args: Any, **kwargs: Any) -> Any:
    return await subjob_completion.handle_scholar_completion(
        *args,
        **kwargs,
        dependencies=completion_composition.scholar_completion_dependencies(
            main.app.state.resources
        ),
    )


async def handle_delegation_child_completion(*args: Any, **kwargs: Any) -> Any:
    return await subjob_completion.handle_delegation_child_completion(
        *args,
        **kwargs,
        dependencies=completion_composition.delegation_completion_dependencies(
            main.app.state.resources
        ),
    )


async def set_target_to_autonomy_status(*args: Any, **kwargs: Any) -> Any:
    return await subjob_completion.set_target_to_autonomy_status(
        *args,
        **kwargs,
        dependencies=completion_composition.scholar_completion_dependencies(
            main.app.state.resources
        ),
    )


async def escalate_target(*args: Any, **kwargs: Any) -> Any:
    return await subjob_completion.escalate_target(
        *args,
        **kwargs,
        dependencies=completion_composition.scholar_completion_dependencies(
            main.app.state.resources
        ),
    )


async def trigger_verification_on_complete(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.trigger_verification_on_complete(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def materialize_critic_verdict_transactional(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.materialize_critic_verdict_transactional(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def run_critic_verdict_followups(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.run_critic_verdict_followups(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def handle_critic_verdict_on_complete(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.handle_critic_verdict_on_complete(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def materialize_verification_critic_transactional(
    *args: Any, **kwargs: Any
) -> Any:
    return await verification_workflow.materialize_verification_critic_transactional(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def run_verification_critic_handoff(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.run_verification_critic_handoff(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


async def record_verification_round(*args: Any, **kwargs: Any) -> Any:
    store = kwargs.pop("postgres_db", None)
    dependencies = completion_composition.verification_dependencies(
        main.app.state.resources
    )
    if store is not None:
        dependencies = dataclasses.replace(dependencies, store=store)
    return await verification_workflow.record_verification_round(
        *args,
        **kwargs,
        dependencies=dependencies,
    )


async def record_completion_decision(*args: Any, **kwargs: Any) -> Any:
    return await verification_workflow.record_completion_decision(
        *args,
        **kwargs,
        dependencies=completion_composition.verification_dependencies(
            main.app.state.resources
        ),
    )


def verification_gate_decision(*args: Any, **kwargs: Any) -> Any:
    return verification_workflow.verification_gate_decision(*args, **kwargs)


def verification_rounds(*args: Any, **kwargs: Any) -> Any:
    return verification_workflow.verification_rounds(*args, **kwargs)


def parse_completion_decision(*args: Any, **kwargs: Any) -> Any:
    return verification_workflow.parse_completion_decision(*args, **kwargs)


async def run_completion_effect(*args: Any, **kwargs: Any) -> Any:
    return await completion_effects.run_completion_effect(*args, **kwargs)


async def run_completion_workspace_teardown(*args: Any, **kwargs: Any) -> Any:
    return await completion_effects.run_completion_workspace_teardown(
        *args,
        **kwargs,
        dependencies=completion_composition.completion_effect_dependencies(
            main.app.state.resources
        ),
    )


async def next_output_ordinal(*args: Any, **kwargs: Any) -> Any:
    return await subjob_output.next_output_ordinal(
        *args,
        **kwargs,
        dependencies=completion_composition.subjob_output_dependencies(
            main.app.state.resources
        ),
    )


async def notify_operator_freeze(*args: Any, **kwargs: Any) -> Any:
    return await job_freeze_notifications.notify_operator_freeze(
        *args,
        **kwargs,
        dependencies=workflows_composition.job_freeze_notification_dependencies(
            main.app.state.resources
        ),
    )


async def trigger_curation_final_pass(*args: Any, **kwargs: Any) -> Any:
    return await curation_final_pass.trigger_curation_final_pass(
        *args,
        **kwargs,
        dependencies=workflows_composition.curation_final_pass_dependencies(
            main.app.state.resources
        ),
    )
