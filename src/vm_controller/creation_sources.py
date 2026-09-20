"""Exact golden clone inputs and non-expiring source retention on the source DV.

Pin publication and every golden deletion compete on the same UID/resourceVersion.
This protects the asynchronous CDI consumer without holding database locks for I/O.
"""

import asyncio
from copy import deepcopy
import json
from uuid import UUID

from kubernetes.client.exceptions import ApiException

from shared.vm_creation_issuance import (
    validate_rootdisk_source,
    public_effect_observation,
)

PINS = "srw.io/vm-create-source-pins"
RELEASED_PIN_LIMIT = 32


class GoldenWaiting(RuntimeError):
    pass


def pins(dv):
    value = json.loads(dv.get("metadata", {}).get("annotations", {}).get(PINS, "{}"))
    if not isinstance(value, dict):
        raise ValueError("Golden source holds are unproven")
    for key, pin in value.items():
        if not isinstance(pin, dict) or pin.get("state") not in {"active", "released"}:
            raise ValueError("Golden source hold identity is unproven")
        identities = {"job_id", "provision_generation", "pvc_uid", "dv_uid"}
        if pin["state"] == "released":
            identities |= {"rootdisk_uid", "rootdisk_pvc_uid"}
        if set(pin) != identities | {"state", "rootdisk_name"}:
            raise ValueError("Golden source hold is incomplete")
        if any(
            str(UUID(item)) != item
            for item in [key, *(pin[field] for field in identities)]
        ):
            raise ValueError("Golden source hold identity is unproven")
        if (
            pin["dv_uid"] != dv["metadata"]["uid"]
            or pin["rootdisk_name"] != "agent-vm-" + pin["job_id"] + "-rootdisk"
        ):
            raise ValueError("Golden source hold association changed")
    return value


class CreationSourcePins:
    def __init__(self, controller):
        from vm_controller import controller as settings
        from vm_controller.creation_actuation import CreationActuator

        self.controller = controller
        self.settings = settings
        self.namespace = settings.VM_NAMESPACE
        self.reader = CreationActuator(controller)

    def pin(self, row, source):
        return {
            "state": "active",
            "job_id": row["job_id"],
            "provision_generation": row["provision_generation"],
            "pvc_uid": source["pvc_uid"],
            "dv_uid": source["dv_uid"],
            "rootdisk_name": "agent-vm-" + row["job_id"] + "-rootdisk",
        }

    async def replace(self, dv):
        return await asyncio.to_thread(
            self.controller.k8s_client.replace_namespaced_custom_object,
            group="cdi.kubevirt.io",
            version="v1beta1",
            plural="datavolumes",
            namespace=self.namespace,
            name=dv["metadata"]["name"],
            body=dv,
        )

    async def hold(self, row, source, dv):
        # A conflicting update is reread by the next bounded create poll. It
        # never converts a stale source UID or a lost reply into a new choice.
        current = pins(dv)
        expected = self.pin(row, source)
        if row["request_id"] in current:
            if current[row["request_id"]] != expected:
                raise ValueError("Golden source hold changed")
            return source
        # The DV was read before this fresh durable check. A completed root
        # effect permanently fences a new pin, including after its tombstone is
        # compacted. Completion racing this check must change the same DV RV
        # through pin publication/release, so the stale replacement loses CAS.
        fresh = await self.reader.authority("inspect", request_id=row["request_id"])
        if (
            any(
                fresh.get(key) != row.get(key)
                for key in (
                    "request_id",
                    "job_id",
                    "provision_generation",
                    "request_digest",
                    "controller_configuration_digest",
                    "expected_pvc_uid",
                )
            )
            or fresh.get("state") != "reconciling"
        ):
            raise ValueError("Golden pin request is no longer current")
        if any(
            effect["state"] != "rejected"
            or effect["carrier_intent"]["effect_kind"] != "rootdisk"
            for effect in fresh["effects"]
        ):
            raise ValueError("Golden pin is fenced by durable issuance")
        current[row["request_id"]] = expected
        body = deepcopy(dv)
        body["metadata"].setdefault("annotations", {})[PINS] = json.dumps(
            current, sort_keys=True, separators=(",", ":")
        )
        try:
            await self.replace(body)
        except Exception:
            # Only exact read-back may settle a lost pin publication response.
            fresh = await self.reader.read("rootdisk", source["name"])
            if (
                not fresh
                or fresh["metadata"]["uid"] != source["dv_uid"]
                or pins(fresh).get(row["request_id"]) != expected
            ):
                raise
        await self.validate(row, source)
        return source

    async def completed_root(self, row):
        effect = next(
            (
                effect
                for effect in row["effects"]
                if effect["state"] == "observed"
                and effect["carrier_intent"]["effect_kind"] == "rootdisk"
            ),
            None,
        )
        if effect is None:
            return
        values = effect["carrier_intent"]
        source = values.get("rootdisk_source", {})
        if (
            source.get("kind") not in {"golden", "prepared"}
            or source.get("mode") == "retained"
        ):
            return
        root = await self.reader.read("rootdisk", values["object_name"])
        pvc = await self.reader.read("pvc", values["object_name"])
        if root is None or pvc is None:
            return
        evidence = public_effect_observation(
            values,
            {"metadata": {"namespace": source["namespace"]}},
            {"outcome": "observed", "object": root, "pvc": pvc},
        )
        if evidence != effect["evidence"]:
            raise ValueError("Completed clone identity changed")
        if (
            root.get("status", {}).get("phase") != "Succeeded"
            or pvc.get("status", {}).get("phase") != "Bound"
        ):
            return
        return source, values, evidence

    async def release_completed(self, row):
        """Release only a recorded, exactly reread CDI-completed clone.

        Keep at most 32 released tombstones on this source UID. A delayed
        publisher loses its resourceVersion CAS; a new pin first checks the
        durable root effect, which fences it even after tombstone compaction.
        """
        completed = await self.completed_root(row)
        if completed is None:
            return
        source, values, evidence = completed
        dv = await self.reader.read("rootdisk", source["name"])
        if dv is None:
            return
        if dv["metadata"]["uid"] != source["dv_uid"]:
            raise ValueError("Completed clone source UID changed")
        current = pins(dv)
        expected = self.pin(row, source)
        released = {
            **expected,
            "state": "released",
            "rootdisk_uid": evidence["uid"],
            "rootdisk_pvc_uid": evidence["pvc_uid"],
        }
        if current.get(row["request_id"]) in (None, released):
            # Missing after compaction is safe: this exact durable root effect
            # and completed clone were revalidated above; never republish it.
            return
        if current.get(row["request_id"]) != expected:
            raise ValueError("Completed clone source hold changed")
        current[row["request_id"]] = released
        tombstones = sorted(
            key
            for key, pin in current.items()
            if pin["state"] == "released" and key != row["request_id"]
        )
        for key in tombstones[: max(0, len(tombstones) + 1 - RELEASED_PIN_LIMIT)]:
            del current[key]
        body = deepcopy(dv)
        body["metadata"].setdefault("annotations", {})[PINS] = json.dumps(
            current, sort_keys=True, separators=(",", ":")
        )
        await self.replace(body)

    async def delete(self, name, *, expected_uid=None):
        dv = await self.reader.read("rootdisk", name)
        if dv is None:
            return
        meta = dv["metadata"]
        if expected_uid is not None and meta.get("uid") != expected_uid:
            raise ValueError("Golden source UID changed")
        if (
            not meta.get("uid")
            or not meta.get("resourceVersion")
            or any(pin["state"] != "released" for pin in pins(dv).values())
        ):
            raise ValueError("Golden source is held or unproven")
        roots = await asyncio.to_thread(
            self.controller.k8s_client.list_namespaced_custom_object,
            group="cdi.kubevirt.io",
            version="v1beta1",
            plural="datavolumes",
            namespace=self.namespace,
            label_selector="srw.io/rootdisk",
        )
        if not isinstance(roots.get("items"), list):
            raise ValueError("Golden source consumers are unproven")
        if any(
            item.get("spec", {}).get("source", {}).get("pvc", {}).get("name") == name
            and item.get("spec", {})
            .get("source", {})
            .get("pvc", {})
            .get("namespace", self.namespace)
            == self.namespace
            for item in roots["items"]
        ):
            raise ValueError("Golden source has a standalone clone consumer")
        try:
            await asyncio.to_thread(
                self.controller.k8s_client.delete_namespaced_custom_object,
                group="cdi.kubevirt.io",
                version="v1beta1",
                plural="datavolumes",
                namespace=self.namespace,
                name=name,
                body={
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "preconditions": {
                        "uid": meta["uid"],
                        "resourceVersion": meta["resourceVersion"],
                    },
                },
            )
        except ApiException as exc:
            if exc.status != 404:
                raise


class GoldenSources(CreationSourcePins):
    async def facts(self, row, name):
        dv, pvc = (
            await self.reader.read("rootdisk", name),
            await self.reader.read("pvc", name),
        )
        if dv is None or pvc is None:
            raise GoldenWaiting("Golden source identity is pending")
        dm, pm = dv["metadata"], pvc["metadata"]
        if any(
            metadata.get("name") != name
            or metadata.get("namespace") != self.namespace
            or metadata.get("deletionTimestamp")
            for metadata in (dm, pm)
        ):
            raise ValueError("Golden source identity changed")
        if (
            dv.get("status", {}).get("phase") != "Succeeded"
            or pvc.get("status", {}).get("phase") != "Bound"
        ):
            raise GoldenWaiting("Golden source is not ready")
        if not any(
            ref.get("kind") == "DataVolume"
            and ref.get("uid") == dm["uid"]
            and ref.get("controller") is True
            for ref in pm.get("ownerReferences", [])
        ):
            raise ValueError("Golden PVC source association changed")
        source = {
            "kind": "golden",
            "image": row["request"]["vm_image"],
            "namespace": self.namespace,
            "name": name,
            "dv_uid": dm["uid"],
            "pvc_uid": pm["uid"],
            "image_ref": dm.get("annotations", {}).get("srw.io/vm-image-ref"),
            "registry_source": dv.get("spec", {}).get("source"),
            "storage": dv.get("spec", {}).get("storage"),
            "pvc_volume_mode": pvc.get("spec", {}).get("volumeMode", "Filesystem"),
            "pvc_owner_dv_uid": dm["uid"],
        }
        validate_rootdisk_source(
            source,
            request=row["request"],
            configuration={
                "namespace": self.namespace,
                "storage_class": self.settings.VM_STORAGE_CLASS,
                "golden_disk_size": self.settings.VM_GOLDEN_DISK_SIZE,
                "golden_enabled": self.settings.VM_GOLDEN_IMAGE_ENABLED,
            },
            expected_pvc_uid=row["expected_pvc_uid"],
        )
        return source, dv

    async def prepare(self, row, frozen=None):
        if row["expected_pvc_uid"] is not None:
            return {"kind": "retained", "pvc_uid": row["expected_pvc_uid"]}
        if frozen is not None:
            if frozen["kind"] != "golden":
                return frozen
            source, dv = await self.facts(row, frozen["name"])
            if source != frozen:
                raise ValueError("Frozen golden source changed")
        elif not self.settings.VM_GOLDEN_IMAGE_ENABLED:
            return {"kind": "registry", "image": row["request"]["vm_image"]}
        else:
            name, waiting = await self.controller._golden_state_nowait(
                row["request"]["vm_image"]
            )
            if waiting is not None or name is None:
                raise GoldenWaiting("Golden import is pending")
            source, dv = await self.facts(row, name)
        return await self.hold(row, source, dv)

    async def validate(self, row, source):
        if source["kind"] != "golden":
            return
        actual, dv = await self.facts(row, source["name"])
        if actual != source or pins(dv).get(row["request_id"]) != self.pin(row, source):
            raise ValueError("Golden source hold changed")


def source_manager(controller, row):
    if row["request"].get("preparation") is not None:
        from vm_controller.creation_preparation import PreparedSources

        return PreparedSources(controller)
    return GoldenSources(controller)
