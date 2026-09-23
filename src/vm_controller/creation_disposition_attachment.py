"""Exact cancellation-only attachment CAS; SQL parent and instance stay held."""

import asyncio
from copy import deepcopy

from shared.vm_creation_attachment_disposition import (
    DISPOSITION_ANNOTATION,
    attachment_completion,
    lease_identity,
    marker,
)
from shared.vm_creation_issuance import REQUEST_ANNOTATION
from shared.vm_workspace_storage import storage_labels, storage_name
from vm_controller.creation_disposition_resources import DispositionResources


class DispositionAttachment:
    def __init__(self, actuator, row, carrier, disposition):
        self.actuator, self.row, self.carrier, self.disposition = (
            actuator,
            row,
            carrier,
            disposition,
        )

    async def fresh(self):
        fresh = await self.actuator.authority(
            "inspect", request_id=self.row["request_id"]
        )
        if (
            fresh["state"] != "cancel_requested"
            or fresh["cancellation_disposition"] != self.disposition
            or fresh["effects"] != self.row["effects"]
        ):
            raise ValueError("Cancellation attachment authority changed")
        await DispositionResources(
            self.actuator, fresh, self.carrier, self.disposition
        ).run()
        return fresh

    async def run(self):
        binding = self.disposition["workspace_storage"]
        name = storage_name(binding) if binding else None
        obj = await self.actuator.read("lease", name) if name else None
        grant = await self.actuator.authority(
            "authorize-disposition",
            request_id=self.row["request_id"],
            carrier=self.carrier,
            stage="workspace_attachment",
            target=obj,
        )
        plan = grant["plan"]
        if (
            grant["operation"] != "dispose_attachment"
            or plan["request_id"] != self.row["request_id"]
            or plan["disposition_id"] != self.disposition["disposition_id"]
            or plan["binding"] != binding
        ):
            raise ValueError("Attachment cancellation plan changed")
        try:
            actual = attachment_completion(plan, obj)
        except ValueError:
            if obj is not None:
                if (
                    lease_identity(obj, name=name, namespace=self.actuator.namespace)
                    != plan["prior"]
                ):
                    raise ValueError("Attachment cancellation prior changed") from None
            elif plan["prior"] is not None:
                raise ValueError(
                    "Attachment cancellation identity disappeared"
                ) from None
            await self.fresh()
            body = (
                deepcopy(obj)
                if obj
                else {
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {
                        "name": name,
                        "namespace": self.actuator.namespace,
                        "labels": storage_labels(binding, self.row["job_id"]),
                    },
                    "spec": {},
                }
            )
            annotations = body["metadata"].setdefault("annotations", {})
            annotations.update(
                {
                    REQUEST_ANNOTATION: self.row["request_id"],
                    "srw.io/provision-generation": self.row["provision_generation"],
                    "srw.io/detached": "true"
                    if plan["outcome"] == "detached"
                    else "false",
                    "srw.io/released": "true"
                    if plan["outcome"] == "released"
                    else "false",
                    DISPOSITION_ANNOTATION: marker(plan),
                }
            )
            kwargs = {"namespace": self.actuator.namespace, "body": body}
            method = self.actuator.controller.coordination_api.create_namespaced_lease
            if obj is not None:
                kwargs["name"] = name
                method = (
                    self.actuator.controller.coordination_api.replace_namespaced_lease
                )
            try:
                await asyncio.to_thread(method, **kwargs)
            except Exception:
                pass  # Lost reply is accepted only by the exact marker readback.
            obj = await self.actuator.read("lease", name)
            actual = attachment_completion(plan, obj)
        await self.fresh()
        recorded = await self.actuator.authority(
            "record-disposition",
            request_id=self.row["request_id"],
            carrier=self.carrier,
            stage="workspace_attachment",
            evidence=obj if obj is not None else {"outcome": "not_applicable"},
        )
        if recorded.get("recorded") is not True or recorded.get("evidence") != actual:
            raise ValueError("Attachment cancellation completion remains unproven")
        return actual
