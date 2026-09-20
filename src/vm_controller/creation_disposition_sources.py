"""Execute an immutable source intent; it never completes creation cancellation."""

from copy import deepcopy
import json

from vm_controller.creation_disposition_resources import require_no_consumers
from vm_controller.creation_sources import (
    PINS,
    RELEASED_PIN_LIMIT,
    pins,
    source_manager,
)


class DispositionSources:
    def __init__(self, actuator, row, carrier, disposition):
        self.actuator, self.row, self.carrier, self.disposition = (
            actuator,
            row,
            carrier,
            disposition,
        )
        self.sources = source_manager(actuator.controller, row)

    async def target_safe(self, target):
        if target is None:
            return
        if target["kind"] == "rootdisk_completed":
            from shared.vm_creation_source_disposition import completed_source_target

            if (
                completed_source_target(
                    self.disposition, await self.target_observation(target["name"])
                )
                != target
            ):
                raise ValueError("Completed retained clone changed")
        elif target["kind"] not in {"rootdisk_never_issued", "rootdisk_purged"}:
            raise ValueError("Source target disposition is unproven")
        elif any(
            [
                await self.actuator.read("rootdisk", target["name"]) is not None,
                await self.actuator.read("pvc", target["name"]) is not None,
            ]
        ):
            raise ValueError("Source target still exists")
        scan = deepcopy(self.disposition)
        scan["objects"]["rootdisk"] = {"name": target["name"]}
        await require_no_consumers(self.actuator, scan)

    async def target_observation(self, name):
        return {
            "outcome": "observed",
            "object": await self.actuator.read("rootdisk", name),
            "pvc": await self.actuator.read("pvc", name),
        }

    async def run(self):
        existing = self.row["cancellation_progress"].get("source")
        source = self.disposition["source"]
        if (
            existing is None
            and source is None
            and self.disposition["source_resolution"] != "not_required"
        ):
            if self.row["request"].get("preparation") is not None:
                allocation = await self.sources.allocation(self.row)
                if allocation.state.get("workspace_source_issued") is False:
                    source = {
                        "kind": "preparation_never_delivered",
                        "allocation": {
                            "name": allocation.name,
                            "uid": allocation.uid,
                            "resource_version": allocation.version,
                            "request": deepcopy(allocation.request),
                            "state": deepcopy(allocation.state),
                        },
                    }
                else:
                    source, _ = await self.sources.facts(self.row)
                    if allocation.state.get("creation_source") != source:
                        raise ValueError("Delivered preparation source is unproven")
            else:
                from vm_controller import controller as settings

                source, _ = await self.sources.facts(
                    self.row, settings._golden_name(self.row["request"]["vm_image"])
                )
        target = None
        if (
            existing is None
            and self.disposition["disk_policy"] == "retain"
            and self.disposition["objects"].get("rootdisk")
        ):
            target = await self.target_observation(
                self.disposition["objects"]["rootdisk"]["name"]
            )
        grant = await self.actuator.authority(
            "authorize-disposition",
            request_id=self.row["request_id"],
            carrier=self.carrier,
            stage="source",
            source=source if existing is None else None,
            target=target,
        )
        plan = grant["plan"]
        if (
            grant["operation"] != "dispose_source"
            or plan["kind"] != "source_disposition_planned"
            or any(
                plan[key] != self.row[key]
                for key in (
                    "request_id",
                    "job_id",
                    "provision_generation",
                    "request_digest",
                    "controller_configuration_digest",
                )
            )
            or plan["disposition_id"] != self.disposition["disposition_id"]
        ):
            raise ValueError("Source disposition intent changed")
        if plan["source"] and plan["source"]["kind"] == "preparation_never_delivered":
            await self.target_safe(plan["target"])
            await self.sources.service.mark_source_never_delivered(
                self.row["request"]["preparation"], plan=plan
            )
            return
        if plan["tombstone"] is None:
            return
        if plan["source"]["kind"] == "prepared":
            from vm_controller.creation_preparation import creation_binding

            allocation = await self.sources.allocation(self.row)
            if (
                allocation.uid != plan["source"]["allocation"]["uid"]
                or allocation.state.get("creation_source") != plan["source"]
                or allocation.state.get("creation_binding")
                != creation_binding(self.row, namespace=self.actuator.namespace)
            ):
                raise ValueError("Prepared source disposition allocation changed")
        await self.target_safe(plan["target"])
        source_dv = await self.release_pin(plan)
        if plan["source"]["kind"] == "prepared":
            await self.sources.service.mark_source_disposed(
                self.row["request"]["preparation"], plan=plan, source_dv=source_dv
            )

    async def release_pin(self, plan):
        source, expected = plan["source"], plan["tombstone"]
        dv = await self.actuator.read("rootdisk", source["name"])
        pvc = await self.actuator.read("pvc", source["name"])
        if (
            dv is None
            or pvc is None
            or any(
                obj["metadata"].get(key) != value
                for obj, uid in ((dv, source["dv_uid"]), (pvc, source["pvc_uid"]))
                for key, value in (
                    ("uid", uid),
                    ("name", source["name"]),
                    ("namespace", source["namespace"]),
                )
            )
            or not dv["metadata"].get("resourceVersion")
            or not any(
                ref.get("kind") == "DataVolume"
                and ref.get("uid") == source["dv_uid"]
                and ref.get("controller") is True
                for ref in pvc["metadata"].get("ownerReferences", [])
            )
        ):
            raise ValueError("Source disposition identity changed")
        current = pins(dv)
        prior = current.get(self.row["request_id"])
        if prior == expected:
            return dv
        active = self.sources.pin(self.row, source)
        allowed = [None, active]
        if plan["target"]["kind"] in {"rootdisk_purged", "rootdisk_completed"}:
            allowed.append(
                {
                    **active,
                    "state": "released",
                    "rootdisk_uid": plan["target"]["uid"],
                    "rootdisk_pvc_uid": plan["target"]["pvc_uid"],
                }
            )
        if prior not in allowed:
            raise ValueError("Source disposition pin changed")
        # Read source RV first, then immutable SQL intent, then CAS that same RV.
        # Even an absent pin requires CAS to fence a pre-cancellation publisher.
        fresh = await self.actuator.authority(
            "inspect", request_id=self.row["request_id"]
        )
        if (
            fresh["state"] != "cancel_requested"
            or fresh["cancellation_progress"].get("source") != plan
        ):
            raise ValueError("Source disposition authority changed")
        await self.target_safe(plan["target"])
        current[self.row["request_id"]] = expected
        terminal = sorted(
            key
            for key, pin in current.items()
            if pin["state"] in {"released", "disposed"}
            and key != self.row["request_id"]
        )
        for key in terminal[: max(0, len(terminal) + 1 - RELEASED_PIN_LIMIT)]:
            del current[key]
        body = deepcopy(dv)
        body["metadata"].setdefault("annotations", {})[PINS] = json.dumps(
            current, sort_keys=True, separators=(",", ":")
        )
        try:
            await self.sources.replace(body)
        except Exception:
            # A lost response is accepted only by exact tombstone readback below.
            pass
        observed = await self.actuator.read("rootdisk", source["name"])
        if (
            observed is None
            or observed["metadata"]["uid"] != source["dv_uid"]
            or pins(observed).get(self.row["request_id"]) != expected
        ):
            raise ValueError("Source disposition CAS remains unproven")
        return observed
