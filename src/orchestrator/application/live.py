"""Live-session composition; knowledge-base/knowledge/features/live_session_jobs.md §9.1."""

import functools

from orchestrator.application.resources import ApplicationResources
from orchestrator.routers.live import LiveDependencies
from orchestrator.services.connector_refs import resolve_execution_connectors
from orchestrator.services.live.admission import LiveAdmissionDependencies
from orchestrator.services.live.inbound import LiveInboundDependencies
from orchestrator.services.live.pods import LivePodClient
from orchestrator.services.live.store import LiveSessionStore


def live_admission_dependencies(
    resources: ApplicationResources, *, session_origin: str = "api"
) -> LiveAdmissionDependencies:
    """A live-session job's admission stage (§4.2)."""
    return LiveAdmissionDependencies(
        db=resources.postgres_db,
        resolve_connector_refs=functools.partial(
            resolve_execution_connectors, resources.postgres_db
        ),
        pods=LivePodClient(resources.postgres_db),
        session_origin=session_origin,
    )


def live_inbound_dependencies(
    resources: ApplicationResources,
) -> LiveInboundDependencies:
    """The webhook path: a live automation's job is admitted as its owner,
    through the same admission as ``POST /api/jobs`` (§3.1)."""
    from orchestrator.application import jobs as jobs_composition

    return LiveInboundDependencies(
        db=resources.postgres_db,
        pods=LivePodClient(resources.postgres_db),
        tasks=resources.live_tasks,
        admit_as_owner=functools.partial(jobs_composition.create_bench_job, resources),
    )


def live_dependencies(resources: ApplicationResources) -> LiveDependencies:
    return LiveDependencies(
        store=LiveSessionStore(resources.postgres_db),
        inbound=live_inbound_dependencies(resources),
    )
