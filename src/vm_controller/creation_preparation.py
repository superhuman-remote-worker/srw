"""Bind the existing preparation allocation to one immutable create source."""

from copy import deepcopy
from dataclasses import asdict

from shared.vm_creation_issuance import validate_rootdisk_source
from vm_controller.creation_sources import CreationSourcePins, pins
from vm_controller.preparation_store import DISK_LABEL, SCOPE_LABEL, scope_key
from vm_controller.workspace_preparation import allocation_name


class PreparedWaiting(RuntimeError):
    pass


def creation_binding(row):
    return {
        key: row[key]
        for key in ("request_id", "provision_generation", "request_digest")
    }


def observed_preparation_metadata(row, vm):
    """Status may project a receipt only for this ledger's exact observed VM."""
    from shared.vm_creation_issuance import (
        prepared_source_metadata,
        validate_prepared_vm_metadata,
    )

    effect = row["effects"][-1] if row.get("effects") else None
    metadata = vm["metadata"]
    if (
        effect is None
        or effect["state"] != "observed"
        or effect["carrier_intent"]["effect_kind"] != "vm"
        or effect["evidence"]["uid"] != metadata["uid"]
        or metadata.get("annotations", {}).get("srw.io/vm-create-request-id")
        != row["request_id"]
        or metadata.get("labels", {}).get("srw.io/owner-id") != row["job_id"]
    ):
        raise ValueError("Prepared VM observation is unproven")
    source = effect["carrier_intent"].get("rootdisk_source", {})
    if source.get("kind") != "prepared":
        raise ValueError("Prepared VM source is unproven")
    validate_prepared_vm_metadata(source, vm)
    return prepared_source_metadata(source)


class PreparedSources(CreationSourcePins):
    @property
    def service(self):
        return self.controller._workspace_preparation()

    def check(self, row, source):
        validate_rootdisk_source(
            source,
            request=row["request"],
            configuration={
                "namespace": self.namespace,
                "preparation": asdict(self.service.settings),
            },
            expected_pvc_uid=row["expected_pvc_uid"],
        )

    async def allocation(self, row):
        request = row["request"]["preparation"]
        allocation = await self.service.store.get(allocation_name(request))
        if allocation is None or allocation.request != request:
            raise ValueError("Prepared allocation identity changed")
        return allocation

    async def facts(self, row):
        allocation = await self.allocation(row)
        if (
            allocation.state.get("creation_binding") != creation_binding(row)
            or allocation.state.get("phase") not in {"Cloning", "Allocated"}
            or allocation.state.get("workspace_source_issued") is not True
        ):
            raise ValueError("Prepared allocation delivery is unproven")
        artifact = await self.service.store.get(allocation.state["artifact"])
        if (
            artifact is None
            or artifact.uid != allocation.state["artifact_uid"]
            or artifact.state.get("phase") != "Ready"
            or artifact.state.get("terminal") is not True
        ):
            raise ValueError("Prepared artifact readiness changed")
        name = artifact.state["disk"]
        dv, pvc = (
            await self.reader.read("rootdisk", name),
            await self.reader.read("pvc", name),
        )
        if dv is None or pvc is None:
            raise ValueError("Prepared source disappeared")
        dm, pm = dv["metadata"], pvc["metadata"]
        labels = {
            DISK_LABEL: artifact.uid,
            SCOPE_LABEL: scope_key(artifact.request["scope"]),
        }
        if (
            any(
                meta.get("name") != name
                or meta.get("namespace") != self.namespace
                or meta.get("deletionTimestamp")
                or any(meta.get("labels", {}).get(k) != v for k, v in labels.items())
                for meta in (dm, pm)
            )
            or dm.get("uid") != artifact.state["dv_uid"]
            or pm.get("uid") != artifact.state["pvc_uid"]
            or not dm.get("resourceVersion")
            or dv.get("status", {}).get("phase") != "Succeeded"
            or pvc.get("status", {}).get("phase") != "Bound"
            or pvc.get("spec", {}).get("volumeMode", "Filesystem") != "Filesystem"
            or not any(
                ref.get("kind") == "DataVolume"
                and ref.get("uid") == dm["uid"]
                and ref.get("controller") is True
                for ref in pm.get("ownerReferences", [])
            )
        ):
            raise ValueError("Prepared disk identity changed")
        source = {
            "kind": "prepared",
            "mode": "clone",
            "namespace": self.namespace,
            "name": name,
            "dv_uid": dm["uid"],
            "pvc_uid": pm["uid"],
            "allocation": {
                "name": allocation.name,
                "uid": allocation.uid,
                "request": deepcopy(allocation.request),
            },
            "artifact": {
                "name": artifact.name,
                "uid": artifact.uid,
                "request": deepcopy(artifact.request),
            },
            "receipt": deepcopy(artifact.state["receipt"]),
        }
        self.check(row, source)
        return source, dv

    async def retained(self, row):
        allocation = await self.allocation(row)
        saved = allocation.state.get("creation_source")
        root = allocation.state.get("creation_root")
        if (
            allocation.state.get("phase") != "Allocated"
            or not isinstance(saved, dict)
            or saved.get("allocation", {}).get("uid") != allocation.uid
            or saved.get("mode") != "clone"
            or not isinstance(root, dict)
        ):
            raise ValueError("Prepared retained completion is unproven")
        source = {
            **deepcopy(saved),
            "mode": "retained",
            "retained_root": deepcopy(root),
        }
        self.check(row, source)
        name, dv, pvc = await self.reader.disk(row)
        if (
            not dv
            or not pvc
            or root
            != {
                "name": name,
                "dv_uid": dv["metadata"]["uid"],
                "pvc_uid": pvc["metadata"]["uid"],
            }
        ):
            raise ValueError("Prepared retained disk changed")
        return source

    async def prepare(self, row, frozen=None):
        # Reusable preparation attachments require a separately proven target
        # binding. Do not substitute the ordinary job root completion contract.
        if row["request"].get("workspace_storage") is not None:
            raise ValueError("Prepared reusable attachment is not yet proven")
        if row["expected_pvc_uid"] is not None:
            source = await self.retained(row)
            if frozen is not None and source != frozen:
                raise ValueError("Frozen retained preparation changed")
            return source
        allocation = await self.service.store.get(
            allocation_name(row["request"]["preparation"])
        )
        saved = allocation.state.get("creation_source") if allocation else None
        if frozen is None and saved is None:
            ready, waiting = await self.service.prepare(
                row["request"]["preparation"], creation=creation_binding(row)
            )
            if waiting is not None:
                allocation = await self.allocation(row)
                if (
                    waiting.get("status") != "waiting_preparation"
                    or allocation.state.get("error")
                    or waiting.get("preparation", {}).get("phase")
                    not in {"Pending", "Importing", "Cloning", "Building", "Releasing"}
                ):
                    raise ValueError("Preparation failed or its source was lost")
                raise PreparedWaiting("Preparation is pending")
            if ready is None:
                raise ValueError("Prepared source is missing")
        source, dv = await self.facts(row)
        if any(value is not None and value != source for value in (frozen, saved)):
            raise ValueError("Frozen prepared source changed")
        if saved is None:
            async with self.service.lock:
                allocation = await self.allocation(row)
                current = allocation.state.get("creation_source")
                if current is not None and current != source:
                    raise ValueError("Prepared source capture changed")
                if allocation.state.get("creation_binding") != creation_binding(row):
                    raise ValueError("Prepared source owner changed")
                await self.service.store.save(
                    allocation, {**allocation.state, "creation_source": source}
                )
        return await self.hold(row, source, dv)

    async def validate(self, row, source):
        if source.get("mode") == "retained":
            if await self.retained(row) != source:
                raise ValueError("Prepared retained proof changed")
            return
        actual, dv = await self.facts(row)
        allocation = await self.allocation(row)
        if (
            actual != source
            or allocation.state.get("creation_source") != source
            or pins(dv).get(row["request_id"]) != self.pin(row, source)
        ):
            raise ValueError("Prepared source hold changed")

    async def release_completed(self, row):
        completed = await self.completed_root(row)
        if completed is None:
            return
        source, values, evidence = completed
        allocation = await self.allocation(row)
        if (
            allocation.state.get("creation_binding") != creation_binding(row)
            or allocation.state.get("creation_source") != source
            or source["allocation"]["uid"] != allocation.uid
        ):
            raise ValueError("Prepared completion allocation changed")
        await self.service.mark_allocated(
            row["request"]["preparation"],
            rootdisk=values["object_name"],
            pvc_uid=evidence["pvc_uid"],
            creation=creation_binding(row),
            creation_source=source,
            rootdisk_dv_uid=evidence["uid"],
        )
        allocation = await self.allocation(row)
        if allocation.state.get("creation_root") != {
            "name": values["object_name"],
            "dv_uid": evidence["uid"],
            "pvc_uid": evidence["pvc_uid"],
        }:
            raise ValueError("Prepared clone completion is pending")
        await super().release_completed(row)
