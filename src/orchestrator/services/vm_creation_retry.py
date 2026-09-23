"""Claimed VM creation reconciliation; all transport happens outside DB locks."""

import asyncio
import logging

from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from orchestrator.services.vm_creation_transport import (
    CreationConfigurationUnavailable,
    dispose_vm_creation,
    replay_vm_creation,
    resolve_vm_creation_configuration,
)

logger = logging.getLogger(__name__)


class VMCreationRetryService:
    def __init__(self, db, provisioner):
        self.db = db
        self.provisioner = provisioner
        self.preflight = VMCreationPreflightStore(db)
        self.store = VMCreationRetryStore(db)

    async def _resolve(self, claim):
        try:
            resolved = await resolve_vm_creation_configuration(
                self.provisioner._http_client,
                claim["request"],
                secret=self.provisioner._lifecycle_hmac_secret,
            )
            await self.preflight.complete_resolution(claim, resolved)
        except CreationConfigurationUnavailable as exc:
            await self.preflight.record_failure(claim, reason=exc.reason)
        except VMCreationRetryConflict:
            # Another control/generation won. The persisted claim expires and
            # the next authority check decides; never create from this result.
            return

    async def _replay(self, claim):
        if claim["state"] == "cancel_requested":
            result = await self.store.settle_never_issued(
                request_id=str(claim["request_id"])
            )
            if result.get("settled") is True:
                return
            # Preparation can publish a source before a carrier exists. Poll
            # cancellation directly; never resume create from this branch.
            try:
                observation = await dispose_vm_creation(
                    self.provisioner._http_client,
                    claim,
                    secret=self.provisioner._lifecycle_hmac_secret,
                )
            except (ValueError, KeyError, TypeError):
                observation = {
                    "outcome": "blocked",
                    "reason": "creation_evidence_unproven",
                }
        else:
            from orchestrator.services.vm_resource_job_runtime import (
                installed_job_resource_store,
            )
            from shared.vm_resource_admission import ResourceAdmissionError

            try:
                async with self.db.acquire() as conn:
                    # A previously issued effect must reach the controller's
                    # exact observation path even after the installed policy
                    # is drained or disabled. This read grants no new effect;
                    # authorize/begin-effect still guard any later issuance.
                    issued = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_creation_effects "
                        "WHERE request_id=$1 AND state IN ('issued','observed'))",
                        claim["request_id"],
                    )
                    resource = (
                        None if issued else await installed_job_resource_store(
                            conn, self.db, claim.get("controller_configuration"),
                        )
                    )
                if resource is not None:
                    admission = await resource.admit(request_id=str(claim["request_id"]))
                    if admission["action"] != "admitted":
                        await self.store.apply_observation(
                            request_id=str(claim["request_id"]),
                            claim_token=str(claim["claim_token"]),
                            expected_revision=claim["revision"],
                            observation={
                                "outcome": "capacity_wait",
                                "reason": admission.get("reason", "resource_wait"),
                            },
                        )
                        return
            except ResourceAdmissionError:
                await self.store.apply_observation(
                    request_id=str(claim["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    expected_revision=claim["revision"],
                    observation={"outcome": "blocked", "reason": "resource_policy_changed"},
                )
                return
            try:
                observation = await replay_vm_creation(
                    self.provisioner._http_client,
                    claim,
                    secret=self.provisioner._lifecycle_hmac_secret,
                )
            except (ValueError, KeyError, TypeError):
                # Local authentication/frozen-input refusal also needs a
                # durable bounded disposition, using this exact observer CAS.
                observation = {
                    "outcome": "blocked",
                    "reason": "creation_evidence_unproven",
                }
            if observation["outcome"] == "adopted":
                # The controller already settled exact adoption via its store
                # endpoint. A stale CAS is expected; no response-driven merge.
                observation = {
                    "outcome": "observation_wait",
                    "reason": "creation_observation_pending",
                }
        await self.store.apply_observation(
            request_id=str(claim["request_id"]),
            claim_token=str(claim["claim_token"]),
            expected_revision=claim["revision"],
            observation=observation,
        )

    @staticmethod
    async def _bounded(tasks):
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, Exception) and not isinstance(
                result, VMCreationRetryConflict
            ):
                # DB/controller exceptions may contain credentials or URLs.
                # Leave the durable claim for expiry; log only the exception type.
                logger.warning(
                    "VM creation reconciliation deferred (%s)", type(result).__name__
                )

    async def _maintain_resource_waiters(self):
        """Nominate a bounded page, then reacquire each genuine source scope."""
        from orchestrator.services.vm_resource_job_runtime import (
            configured_enforcement_policy,
            installed_job_resource_store,
        )
        from orchestrator.services.vm_resource_waiter_maintenance import (
            VMResourceWaiterMaintenance,
        )
        from shared.vm_resource_admission import ResourceAdmissionError

        try:
            selected = configured_enforcement_policy()
        except ResourceAdmissionError:
            # A malformed currently selected policy must not turn the
            # reconciliation loop off. Each frozen v3 claim independently
            # fails closed at admission/issuance, while old effects replay.
            logger.warning("VM resource waiter maintenance held (invalid policy)")
            return
        if selected is None:
            return
        try:
            async with self.db.acquire() as conn:
                mode = await conn.fetchval(
                    "SELECT mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
                    selected.inventory.cluster_id,
                )
                if mode not in {"enforce", "drain"}:
                    return
                resource = await installed_job_resource_store(
                    conn, self.db, {
                        "version": 3,
                        "resource_admission": {
                            "cluster_id": selected.inventory.cluster_id,
                            "policy_digest": selected.policy_digest,
                        },
                    }, fresh=False,
                )
            maintenance = VMResourceWaiterMaintenance(resource)
            candidates = await maintenance.candidates(limit=8)
        except ResourceAdmissionError:
            # A mode/policy race changes no candidate's owner authority. The
            # actual retry scan still runs and independently refuses new v3
            # grants until the installed policy matches.
            logger.warning("VM resource waiter maintenance held (policy changed)")
            return
        await self._bounded([
            maintenance.maintain(request_id=request_id)
            for request_id in candidates
        ])

    async def reconcile_once(self):
        # Feature-off blocks new admission in the caller. Existing intent must
        # continue to reconcile so cancellation and uncertain effects settle.
        await self.preflight.settle_cancelled(limit=20)
        await self._maintain_resource_waiters()
        preflights = await self.preflight.claim_due(limit=4)
        await self._bounded([self._resolve(claim) for claim in preflights])
        claims = await self.store.claim_due(limit=4)
        await self._bounded([self._replay(claim) for claim in claims])

    async def run(self, shutdown_event):
        while not shutdown_event.is_set():
            try:
                await self.reconcile_once()
            except Exception as exc:
                logger.warning("VM creation scan deferred (%s)", type(exc).__name__)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
