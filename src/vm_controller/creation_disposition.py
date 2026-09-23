"""Bounded cancellation discovery under the original SQL creation admission.

Observed new per-Job resources use fixed-UID teardown and typed progress.
Source and retained attachment release and terminal settlement remain held.
"""

from collections.abc import Mapping
from uuid import UUID

from shared.vm_creation_disposition import (
    disposition_identity,
    validate_disposition_request,
)
from shared.vm_creation_issuance import (
    verify_creation_carrier,
    CREATION_SIGNATURE_ANNOTATION,
)
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload, unsigned_payload
from vm_controller.creation_actuation import CreationActuator, carrier_record


class CreationDisposer:
    def __init__(self, controller):
        self.controller = controller
        self.actuator = CreationActuator(controller)

    async def run(self, payload):
        identity = validate_disposition_request(payload)
        pending = {
            **identity,
            "status": "creation_disposition_pending",
            "reason": "creation_disposition_pending",
        }
        if self.actuator.secret is None:
            return {
                **identity,
                "status": "creation_attention",
                "reason": "creation_evidence_unproven",
            }
        try:
            return await self._run(identity, pending)
        except (ValueError, TypeError, KeyError):
            return {
                **identity,
                "status": "creation_attention",
                "reason": "creation_evidence_unproven",
            }
        except Exception:
            # Unknown API/authority results never become absence or a fresh create.
            return pending

    async def _run(self, identity, pending):
        for _ in range(3):
            row = await self.actuator.authority(
                "inspect", request_id=identity["request_id"]
            )
            if (
                disposition_identity(row) != identity
                or canonical_request_digest(row["request"])
                != identity["request_digest"]
                or any(
                    row["request"][key] != identity[key]
                    for key in ("job_id", "provision_generation")
                )
            ):
                raise ValueError("Cancellation identity changed")
            if row["state"] == "settled" and row["reason"] == "creation_adopted":
                return {**identity, "status": "creation_adopted"}
            if row["state"] == "settled" and row["reason"] == "creation_disposed":
                return {**identity, "status": "creation_disposed"}
            if row["state"] != "cancel_requested" or not row["creation_admission_id"]:
                raise ValueError("Cancellation admission is unavailable")
            name = "srw-cleanup-" + UUID(row["creation_admission_id"]).hex
            lease = await self.actuator.read("lease", name)
            if row["creation_carrier_uid"] and (
                lease is None or lease["metadata"]["uid"] != row["creation_carrier_uid"]
            ):
                raise ValueError("Cancellation carrier changed")
            if lease is None or not lease["metadata"].get("annotations", {}).get(
                CREATION_SIGNATURE_ANNOTATION
            ):
                prepared = await self.actuator.authority(
                    "prepare-disposition", request_id=row["request_id"]
                )
                if (
                    prepared.get("actuation_allowed") is not False
                    or prepared["namespace"] != self.actuator.namespace
                ):
                    raise ValueError("Cancellation carrier authority is unproven")
                lease = await self.actuator.publish(prepared["carrier_intent"])
            if lease["metadata"].get("deletionTimestamp") is not None:
                raise ValueError("Cancellation carrier is deleting")
            values = verify_creation_carrier(lease, secret=self.actuator.secret)
            if (
                values["retry_request_id"] != row["request_id"]
                or values["admission_id"] != row["creation_admission_id"]
            ):
                raise ValueError("Cancellation carrier identity changed")
            latest = row["effects"][-1] if row["effects"] else None
            if latest and (
                latest["state"] == "issued"
                or latest["state"] == "observed"
                and latest["carrier_intent"]["effect_kind"] == "vm"
            ):
                from vm_controller.creation_actuation import reconcile_creation_carrier

                await reconcile_creation_carrier(
                    self.controller,
                    carrier_record(lease, secret=self.actuator.secret),
                    _observe_only=True,
                )
                fresh = await self.actuator.authority(
                    "inspect", request_id=row["request_id"]
                )
                if (
                    fresh["effects"] == row["effects"]
                    and fresh["state"] == row["state"]
                ):
                    return pending
                continue
            frozen = await self.actuator.authority(
                "freeze-disposition", request_id=row["request_id"], carrier=lease
            )
            if frozen.get("frozen") is not True:
                return pending
            disposition = frozen["disposition"]
            if (
                disposition["request_id"] != row["request_id"]
                or str(UUID(disposition["disposition_id"]))
                != disposition["disposition_id"]
            ):
                raise ValueError("Cancellation disposition changed")
            from vm_controller.creation_disposition_resources import (
                DispositionResources,
            )

            await DispositionResources(self.actuator, row, lease, disposition).run()
            from vm_controller.creation_disposition_sources import DispositionSources
            from shared.vm_creation_source_completion import validate_source_completion

            completed = row.get("cancellation_completion", {}).get("source")
            if completed is None:
                # A planned key is never completion. Only a separately accepted
                # actual receipt may survive source GC without replaying its CAS.
                actual = await DispositionSources(
                    self.actuator, row, lease, disposition
                ).run()
                recorded = await self.actuator.authority(
                    "record-disposition",
                    request_id=row["request_id"],
                    carrier=lease,
                    stage="source",
                    evidence=actual,
                )
                if recorded.get("recorded") is not True:
                    return pending
                validate_source_completion(actual["plan"], recorded["evidence"])
            else:
                validate_source_completion(
                    row["cancellation_progress"]["source"], completed
                )
            from vm_controller.creation_disposition_attachment import (
                DispositionAttachment,
            )

            await DispositionAttachment(self.actuator, row, lease, disposition).run()
            settled = await self.actuator.authority(
                "settle-disposition", request_id=row["request_id"], carrier=lease
            )
            if settled.get("settled") is True and settled.get("disposition") == "creation_disposed":
                return {**identity, "status": "creation_disposed"}
            return {**pending, "disposition_id": disposition["disposition_id"]}
        return pending


async def http_dispose(controller, request):
    from aiohttp import web
    from vm_controller import controller as settings

    operation = "creation_retry_dispose"
    try:
        payload = await request.json()
    except (ValueError, TypeError):
        return web.json_response({"error": "invalid request"}, status=400)
    if (
        settings.LIFECYCLE_HMAC_SECRET is None
        or not isinstance(payload, Mapping)
        or not await controller._verify_lifecycle_request(
            payload, operation, mutating=True
        )
    ):
        return web.json_response({"error": "authentication failed"}, status=401)
    try:
        result = await CreationDisposer(controller).run(unsigned_payload(payload))
        status = 200
    except (ValueError, TypeError, KeyError):
        result = {
            "status": "creation_attention",
            "reason": "creation_evidence_unproven",
        }
        status = 400
    return web.json_response(
        sign_payload(
            result,
            direction="response",
            operation=operation,
            secret=settings.LIFECYCLE_HMAC_SECRET,
            correlation_id=payload[AUTH_FIELD]["request_id"],
        ),
        status=status,
    )
