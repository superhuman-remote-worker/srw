"""Phone composition; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

from orchestrator.application import jobs as jobs_composition
from orchestrator.application.resources import ApplicationResources, bound
from orchestrator.routers.telephony import TelephonyDependencies
from orchestrator.services import job_dispatcher
from orchestrator.services.telephony.front import TelephonyFront
from orchestrator.services.telephony.recorder import Recorder
from orchestrator.services.telephony.store import TelephonyStore


def telephony_dependencies(resources: ApplicationResources) -> TelephonyDependencies:
    store = TelephonyStore(resources.postgres_db)
    return TelephonyDependencies(
        store=store,
        front=TelephonyFront(
            store,
            resources.telephony_tasks,
            bound(
                job_dispatcher.trigger_dispatch,
                jobs_composition.job_dispatch_dependencies,
                resources,
            ),
        ),
    )


def recorder(resources: ApplicationResources) -> Recorder:
    return Recorder(TelephonyStore(resources.postgres_db), resources.telephony_tasks)
