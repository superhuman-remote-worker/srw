"""Reconciled VM preparation: scoped cache records, one writer, exact disk clones.

The singleton controller serializes state transitions. Kubernetes records retain
all operation identities; restarting the controller never restarts a builder.
"""

import asyncio
from copy import deepcopy
import json
import re
from uuid import UUID

import httpx

from shared.workspace_preparation import (
    PREPARATION_LABEL,
    cache_key,
    image_reference,
    normalized_image,
    validate_request,
)
from vm_controller.preparation_manifests import builder_pod, firewall_command
from vm_controller.preparation_registry import RegistryResolver
from vm_controller.preparation_store import (
    PreparationConflict,
    PreparationStore,
    now,
    record_name,
)

ACTIVE = {"Importing", "Cloning", "Building", "Releasing"}
HELD = {"Pending", "Cloning"}


class PreparationUnavailable(ValueError):
    """An expected, bounded refusal code safe to report to an execution."""


def allocation_name(request):
    return record_name(
        "allocation",
        [
            request["ownerKind"],
            request["allocationId"],
            request.get("runtimeGeneration"),
        ],
    )


def creation_held(allocation):
    """A delivered protocol source stays held until exact clone disposition."""
    if allocation.state.get("workspace_source_issued") is False:
        return False
    binding = allocation.state.get("creation_binding")
    source = allocation.state.get("creation_source")
    binding_has_target = isinstance(binding, dict) and "target" in binding
    source_has_target = isinstance(source, dict) and "target" in source
    # Missing binding metadata cannot downgrade an already delivered workspace
    # source to an ordinary allocation, including the legacy no-binding path.
    if binding_has_target != source_has_target or (
        binding_has_target and binding["target"] != source["target"]
    ):
        return True
    if "creation_binding" not in allocation.state:
        return False
    from shared.vm_preparation_target import creation_root_name

    try:
        expected = creation_root_name(
            allocation.state["creation_binding"], allocation.request
        )
    except (ValueError, TypeError, KeyError):
        return True
    root = allocation.state.get("creation_root")
    if (
        not isinstance(root, dict)
        or set(root) != {"name", "dv_uid", "pvc_uid"}
        or root["name"] != expected
    ):
        return True
    try:
        return any(str(UUID(root[key])) != root[key] for key in ("dv_uid", "pvc_uid"))
    except (ValueError, TypeError, AttributeError):
        return True


class VMWorkspacePreparation:
    def __init__(
        self, controller, *, namespace, storage_class, settings, resolver=None
    ):
        self.settings = settings
        self.store = PreparationStore(
            controller.core_api, controller.k8s_client, namespace, storage_class
        )
        self.resolver = resolver or RegistryResolver(
            hosts=settings.registry_hosts,
            insecure_hosts=settings.insecure_registry_hosts,
            token_hosts=settings.token_hosts,
        )
        self.lock = asyncio.Lock()
        self.builder_image = None
        self.last_allocation_sweep = 0.0

    async def prepare(self, value, *, creation=None):
        request = validate_request(value)
        if creation is not None:
            from shared.vm_preparation_target import creation_root_name

            try:
                creation_root_name(creation, request, namespace=self.store.namespace)
            except (ValueError, TypeError, KeyError) as exc:
                raise PreparationConflict(
                    "Creation allocation binding is invalid."
                ) from exc
        if not self.settings.enabled:
            raise PreparationConflict("VM workspace preparation is not enabled.")
        self.resolver.permitted(request["image"])
        async with self.lock:
            allocation = await self.store.ensure(
                allocation_name(request),
                "allocation",
                request,
                {
                    "phase": "Pending",
                    "workspace_source_issued": False,
                    "expires_at": now() + self.settings.wait_budget + 300,
                },
            )
            bound = allocation.state.get("creation_binding")
            if "creation_binding" in allocation.state and (
                bound is None or bound != creation
            ):
                raise PreparationConflict(
                    "Preparation source belongs to another creation request."
                )
            if creation is not None and bound is None:
                if allocation.state.get("workspace_source_issued") is not False:
                    raise PreparationConflict(
                        "Existing preparation delivery is unproven."
                    )
                await self.store.save(
                    allocation,
                    {**allocation.state, "creation_binding": deepcopy(creation)},
                )
            if allocation.state["phase"] in {"Cancelled", "Failed"}:
                return self._result(allocation, None)
            if not allocation.state.get("artifact"):
                try:
                    await self._bind(allocation)
                except (ValueError, httpx.HTTPError) as exc:
                    await self.store.save(
                        allocation,
                        {
                            **allocation.state,
                            "phase": "Failed",
                            "error": str(exc)
                            if isinstance(exc, PreparationUnavailable)
                            else "ImageResolutionFailed",
                        },
                    )
                    return self._result(allocation, None)
            artifact = await self.store.get(allocation.state["artifact"])
            if artifact is None or artifact.uid != allocation.state["artifact_uid"]:
                await self.store.save(
                    allocation,
                    {
                        **allocation.state,
                        "phase": allocation.state["phase"]
                        if creation_held(allocation)
                        else "Failed",
                        "error": "PreparedArtifactLost",
                    },
                )
                return self._result(allocation, None)
            try:
                await self._advance(artifact)
            except PreparationConflict:
                await self._fail(artifact, "IdentityUnknown", lost=True)
            if artifact.state["phase"] in {"Failed", "Lost", "Deleting"}:
                await self.store.save(
                    allocation,
                    {
                        **allocation.state,
                        "phase": allocation.state["phase"]
                        if creation_held(allocation)
                        else "Failed",
                        "error": artifact.state.get(
                            "error", "PreparedArtifactUnavailable"
                        ),
                    },
                )
            elif artifact.state["phase"] == "Ready":
                await self.store.disk_identity(
                    artifact.state["disk"],
                    owner_uid=artifact.uid,
                    expected=artifact.state["pvc_uid"],
                )
                await self.store.save(artifact, {**artifact.state, "last_used": now()})
                if (
                    allocation.state["phase"] != "Allocated"
                    or allocation.state.get("workspace_source_issued") is not True
                ):
                    await self.store.save(
                        allocation,
                        {
                            **allocation.state,
                            "phase": "Allocated"
                            if allocation.state["phase"] == "Allocated"
                            else "Cloning",
                            # Persist before returning a disk to a VM creator.
                            # Cancellation cannot turn a handed-out source into
                            # proof that no workspace ever existed.
                            "workspace_source_issued": True,
                        },
                    )
            return self._result(allocation, artifact)

    async def _bind(self, allocation):
        request = allocation.request
        if self.builder_image is None:
            image = self.settings.builder_image
            self.resolver.permitted(image)
            self.builder_image = (
                normalized_image(image)
                if image_reference(image)[2].startswith("sha256:")
                else await self.resolver.resolve(image)
            )
        scope = request["scope"]
        tag_identity = {"scope": scope, "image": request["image"]}
        tag = await self.store.get(record_name("tag", tag_identity))
        base_image = tag.state.get("image") if tag else None
        if base_image and not await self._image_cached(scope, base_image):
            base_image = None
        if request["pullPolicy"] == "Always" or base_image is None:
            if request["pullPolicy"] == "Never":
                if image_reference(request["image"])[2].startswith("sha256:"):
                    base_image = request["image"]
                else:
                    raise PreparationUnavailable("ImageNotCached")
            else:
                base_image = await self.resolver.resolve(request["image"])
                tag = await self.store.ensure(
                    record_name("tag", tag_identity),
                    "tag",
                    tag_identity,
                    {"image": base_image},
                )
                await self.store.save(tag, {"image": base_image})
        key = cache_key(
            request,
            base_image=base_image,
            builder_image=self.builder_image,
            disk_size=self.settings.disk_size,
            network_policy=(
                {
                    "revision": self.settings.network_policy_revision,
                    "podFirewall": self.settings.firewall,
                }
                if self.settings.pod_firewall
                else self.settings.network_policy_revision
                if self.settings.network_enabled
                else "offline"
            ),
        )
        artifact_request = {
            "scope": scope,
            "baseImage": base_image,
            "builderImage": self.builder_image,
            "steps": request["steps"],
            "cacheKey": key,
            "networkEnabled": self.settings.network_enabled,
            "diskSize": self.settings.disk_size,
        }
        if self.settings.firewall is not None:
            artifact_request["podFirewall"] = self.settings.firewall
        name = record_name("artifact", key)
        artifact = await self.store.get(name)
        if artifact and artifact.state["phase"] == "Failed":
            if await self._remove(artifact, allow_failed_holders=True):
                artifact = None
            else:
                raise PreparationConflict("Previous failed build is still retiring.")
        if request["pullPolicy"] == "Never" and (
            artifact is None or artifact.state["phase"] != "Ready"
        ):
            base = await self.store.get(
                record_name("base", {"scope": scope, "image": base_image})
            )
            if (
                base is None
                or base.state.get("phase") != "Ready"
                or await self.store.dv(base.state["disk"]) is None
            ):
                raise PreparationUnavailable("ImageNotCached")
        if artifact is None:
            entries = len(await self.store.records("artifact")) + len(
                await self.store.records("base")
            )
            base = await self.store.get(
                record_name("base", {"scope": scope, "image": base_image})
            )
            if entries + (1 if base else 2) > self.settings.max_cache_entries:
                raise PreparationUnavailable("CacheCapacityExceeded")
        artifact = await self.store.ensure(
            name, "artifact", artifact_request, {"phase": "Queued", "last_used": now()}
        )
        await self.store.save(
            allocation,
            {
                **allocation.state,
                "artifact": artifact.name,
                "artifact_uid": artifact.uid,
                "base_image": base_image,
                "cache_hit": artifact.state["phase"] == "Ready",
            },
        )

    async def _image_cached(self, scope, image):
        base = await self.store.get(
            record_name("base", {"scope": scope, "image": image})
        )
        if base and base.state["phase"] not in {"Failed", "Lost", "Deleting"}:
            if await self.store.dv(
                base.state.get("disk", "srw-prep-base-" + UUID(base.uid).hex)
            ):
                return True
        return any(
            a.request["scope"] == scope
            and a.request["baseImage"] == image
            and a.state["phase"] not in {"Failed", "Lost", "Deleting"}
            for a in await self.store.records("artifact")
        )

    async def _base(self, artifact):
        spec = {
            "scope": artifact.request["scope"],
            "image": artifact.request["baseImage"],
        }
        base = await self.store.ensure(
            record_name("base", spec), "base", spec, {"phase": "Importing"}
        )
        if base.state["phase"] in {"Failed", "Deleting"}:
            if await self._remove_base(base, recovering=True):
                base = await self.store.ensure(
                    record_name("base", spec), "base", spec, {"phase": "Importing"}
                )
        if base.state["phase"] in {"Failed", "Lost", "Deleting"}:
            await self._fail(artifact, "BaseImportFailed")
            return None
        disk = "srw-prep-base-" + UUID(base.uid).hex
        if base.state.get("dv_uid") and await self.store.dv(disk) is None:
            await self._fail(base, "BaseIdentityLost", lost=True)
            await self._fail(artifact, "BaseIdentityLost")
            return None
        dv = await self.store.ensure_disk(
            disk,
            owner_uid=base.uid,
            scope=spec["scope"],
            source={"registry": {"url": "docker://" + spec["image"]}},
            size=artifact.request["diskSize"],
        )
        pvc = await self.store.disk_identity(
            disk, owner_uid=base.uid, expected=base.state.get("pvc_uid")
        )
        state = {
            **base.state,
            "disk": disk,
            "dv_uid": dv["metadata"]["uid"],
            "pvc_uid": pvc,
            "last_used": now(),
        }
        phase = dv.get("status", {}).get("phase")
        if (
            phase == "Failed"
            or now() - base.created > self.settings.import_timeout
            and phase != "Succeeded"
        ):
            state.update(phase="Failed", error="BaseImportFailed")
        elif phase == "Succeeded" and pvc:
            state["phase"] = "Ready"
        await self.store.save(base, state)
        if state["phase"] == "Failed":
            await self._fail(artifact, "BaseImportFailed")
        if state["phase"] != "Ready":
            return None
        return base

    async def _advance(self, artifact):
        phase = artifact.state["phase"]
        if phase in {"Ready", "Failed", "Lost", "Deleting"}:
            return
        if phase == "Queued":
            active = [
                row
                for row in await self.store.records("artifact")
                if row.state["phase"] in ACTIVE
                or row.state["phase"] == "Lost"
                and row.state.get("pod")
            ]
            if len(active) >= self.settings.max_concurrent:
                if now() - artifact.created > self.settings.import_timeout:
                    await self._fail(artifact, "BuildCapacityTimeout")
                return
            await self.store.save(artifact, {**artifact.state, "phase": "Importing"})
        if artifact.state["phase"] == "Importing":
            base = await self._base(artifact)
            if base is None:
                return
            await self.store.save(
                artifact,
                {
                    **artifact.state,
                    "phase": "Cloning",
                    "base_record": base.name,
                    "base_uid": base.uid,
                    "base_disk": base.state["disk"],
                    "base_pvc_uid": base.state["pvc_uid"],
                    "clone_started": now(),
                },
            )
        if artifact.state["phase"] == "Cloning":
            await self._clone(artifact)
        if artifact.state["phase"] == "Building":
            await self._observe_builder(artifact)
        if artifact.state["phase"] == "Releasing":
            await self._release_builder(artifact)

    async def _clone(self, artifact):
        base = await self.store.get(artifact.state["base_record"])
        if base is None or base.uid != artifact.state["base_uid"]:
            raise PreparationConflict("Build base record changed.")
        await self.store.disk_identity(
            base.state["disk"],
            owner_uid=base.uid,
            expected=artifact.state["base_pvc_uid"],
        )
        disk = "srw-prepared-" + UUID(artifact.uid).hex
        if artifact.state.get("dv_uid") and await self.store.dv(disk) is None:
            raise PreparationConflict("Build clone identity was lost.")
        dv = await self.store.ensure_disk(
            disk,
            owner_uid=artifact.uid,
            scope=artifact.request["scope"],
            source={
                "pvc": {"namespace": self.store.namespace, "name": base.state["disk"]}
            },
            size=artifact.request["diskSize"],
        )
        pvc = await self.store.disk_identity(
            disk, owner_uid=artifact.uid, expected=artifact.state.get("pvc_uid")
        )
        await self.store.save(
            artifact,
            {
                **artifact.state,
                "disk": disk,
                "dv_uid": dv["metadata"]["uid"],
                "pvc_uid": pvc,
            },
        )
        phase = dv.get("status", {}).get("phase")
        if (
            phase == "Failed"
            or now() - artifact.state["clone_started"] > self.settings.import_timeout
            and phase != "Succeeded"
        ):
            await self._fail(artifact, "BuildCloneFailed")
            return
        if phase != "Succeeded" or not pvc:
            return
        input_request = {
            "version": 1,
            "buildUid": artifact.uid,
            "pvcUid": pvc,
            "cacheKey": artifact.request["cacheKey"],
            "steps": artifact.request["steps"],
            "networkEnabled": artifact.request["networkEnabled"],
        }
        name = "srw-preparer-" + UUID(artifact.uid).hex
        input_record = await self.store.ensure(name, "input", input_request, {})
        # Commit the one create intent before I/O. A missing Pod after this
        # point is not permission to create a replacement writer.
        await self.store.save(
            artifact,
            {
                **artifact.state,
                "phase": "Building",
                "pod": name,
                "input_uid": input_record.uid,
                "build_started": now(),
            },
        )
        raw = await self.store.call(
            self.store.core.create_namespaced_pod,
            body=builder_pod(
                namespace=self.store.namespace,
                name=name,
                uid=artifact.uid,
                disk=disk,
                input_name=name,
                image=artifact.request["builderImage"],
                timeout=self.settings.build_timeout,
                image_pull_secrets=self.settings.image_pull_secrets,
                pod_firewall=artifact.request.get("podFirewall"),
            ),
        )
        if raw is not None:
            await self.store.save(
                artifact, {**artifact.state, "pod_uid": raw.metadata.uid}
            )

    async def _observe_builder(self, artifact):
        pod = await self.store.call(
            self.store.core.read_namespaced_pod, name=artifact.state["pod"]
        )
        if pod is None:
            raise PreparationConflict(
                "Build Pod disappeared without terminal evidence."
            )
        if (
            (pod.metadata.labels or {}).get(PREPARATION_LABEL) != artifact.uid
            or artifact.state.get("pod_uid") not in {None, pod.metadata.uid}
            or len(pod.spec.containers) != 1
            or pod.spec.containers[0].image != artifact.request["builderImage"]
        ):
            raise PreparationConflict("Build Pod identity changed.")
        firewall = artifact.request.get("podFirewall")
        init_containers = pod.spec.init_containers or []
        if firewall is not None:
            if (
                len(init_containers) != 1
                or init_containers[0].name != "network-firewall"
                or init_containers[0].image != artifact.request["builderImage"]
                or init_containers[0].command != firewall_command(firewall)
            ):
                raise PreparationConflict("Build Pod firewall identity changed.")
        elif init_containers:
            raise PreparationConflict("Build Pod has an unexpected init container.")
        if not artifact.state.get("pod_uid"):
            await self.store.save(
                artifact, {**artifact.state, "pod_uid": pod.metadata.uid}
            )
        statuses = pod.status.container_statuses or []
        init_statuses = pod.status.init_container_statuses or []
        if (
            firewall is not None
            and pod.status.phase == "Failed"
            and len(init_statuses) == 1
            and init_statuses[0].name == "network-firewall"
            and init_statuses[0].restart_count == 0
            and init_statuses[0].state.terminated is not None
            and init_statuses[0].state.terminated.exit_code != 0
            and len(statuses) == 1
            and statuses[0].name == "builder"
            and statuses[0].restart_count == 0
            and not statuses[0].container_id
            and statuses[0].state.waiting is not None
            and statuses[0].state.waiting.reason == "PodInitializing"
            and not (statuses[0].last_state and statuses[0].last_state.terminated)
        ):
            # This init container cannot mount the disk; Kubernetes reports
            # that the regular builder never started. Retire the exact Pod
            # before releasing its failed artifact, as for a terminal builder.
            await self.store.save(
                artifact,
                {
                    **artifact.state,
                    "phase": "Releasing",
                    "terminal": True,
                    "receipt": None,
                    "exit_code": init_statuses[0].state.terminated.exit_code,
                    "failure_stage": "PodFirewall",
                },
            )
            return
        if len(statuses) != 1 or statuses[0].state.terminated is None:
            if pod.status.phase in {"Failed", "Succeeded"}:
                # Eviction can lose the process status. Never infer that an
                # unobserved disk writer stopped just because the Pod failed.
                raise PreparationConflict(
                    "Builder termination evidence is unavailable."
                )
            return
        terminated = statuses[0].state.terminated
        receipt = None
        if terminated.exit_code == 0:
            try:
                receipt = json.loads(terminated.message or "")
                expected = {
                    "version": 1,
                    "buildUid": artifact.uid,
                    "pvcUid": artifact.state["pvc_uid"],
                    "cacheKey": artifact.request["cacheKey"],
                    "phase": "Succeeded",
                }
                if (
                    len(terminated.message) > 4096
                    or not isinstance(receipt, dict)
                    or set(receipt) != {*expected, "diskSha256", "diskBytes"}
                    or any(receipt.get(k) != v for k, v in expected.items())
                    or not re.fullmatch(r"[0-9a-f]{64}", receipt.get("diskSha256", ""))
                    or type(receipt.get("diskBytes")) is not int
                    or receipt["diskBytes"] <= 0
                ):
                    receipt = None
            except (ValueError, TypeError):
                receipt = None
        await self.store.save(
            artifact,
            {
                **artifact.state,
                "phase": "Releasing",
                "terminal": True,
                "receipt": receipt,
                "exit_code": terminated.exit_code,
            },
        )

    async def _release_builder(self, artifact):
        if not artifact.state.get("terminal"):
            raise PreparationConflict("Build has no terminal process evidence.")
        pod = await self.store.call(
            self.store.core.read_namespaced_pod, name=artifact.state["pod"]
        )
        if pod is not None:
            if pod.metadata.uid != artifact.state["pod_uid"]:
                raise PreparationConflict("Build Pod was replaced during retirement.")
            await self.store.call(
                self.store.core.delete_namespaced_pod,
                name=artifact.state["pod"],
                body={"preconditions": {"uid": pod.metadata.uid}},
            )
            return
        if not await self.store.unused(artifact.state["disk"]):
            return
        input_record = await self.store.get(artifact.state["pod"])
        if input_record:
            if input_record.uid != artifact.state["input_uid"]:
                raise PreparationConflict("Build input identity changed.")
            await self.store.delete_record(input_record)
        state = {
            **artifact.state,
            "phase": "Ready" if artifact.state.get("receipt") else "Failed",
            "last_used": now(),
        }
        if state.get("cancel_requested"):
            state.update(phase="Failed", error="BuildCancelled")
        elif state["phase"] == "Failed":
            state["error"] = "BuildFailed"
        await self.store.save(artifact, state)

    async def _fail(self, record, error, *, lost=False):
        await self.store.save(
            record,
            {**record.state, "phase": "Lost" if lost else "Failed", "error": error},
        )

    def _result(self, allocation, artifact):
        phase = artifact.state["phase"] if artifact else allocation.state["phase"]
        payload = {
            "allocationId": allocation.request["allocationId"],
            "phase": phase,
            "cacheHit": allocation.state.get("cache_hit", False),
        }
        if "runtimeGeneration" in allocation.request:
            payload["runtimeGeneration"] = allocation.request["runtimeGeneration"]
        if artifact:
            payload.update(
                uid=artifact.uid,
                cacheKey=artifact.request["cacheKey"],
                baseImage=artifact.request["baseImage"],
            )
        if allocation.state["phase"] in {"Failed", "Cancelled"}:
            payload.update(
                phase=allocation.state["phase"],
                error=allocation.state.get("error", "PreparationCancelled"),
            )
            return None, {
                "status": "failed",
                "error": payload["error"],
                "preparation": payload,
            }
        if phase != "Ready":
            return None, {"status": "waiting_preparation", "preparation": payload}
        payload.update(artifact.state["receipt"])
        return {
            "name": artifact.state["disk"],
            "pvc_uid": artifact.state["pvc_uid"],
            "preparation": payload,
        }, None

    async def mark_allocated(
        self,
        request,
        *,
        rootdisk,
        pvc_uid,
        creation=None,
        creation_source=None,
        rootdisk_dv_uid=None,
    ):
        """Release the cache pin only after CDI has finished the workspace clone."""
        request = validate_request(request)
        from shared.vm_preparation_target import creation_root_name

        expected_root = (
            creation_root_name(creation, request, namespace=self.store.namespace)
            if creation is not None
            else None
        )
        async with self.lock:
            allocation = await self.store.get(allocation_name(request))
            if (
                allocation is None
                or allocation.request != request
                or allocation.state["phase"] != "Cloning"
            ):
                return
            bound = "creation_binding" in allocation.state
            if bound and creation is None:
                # Legacy owner/name observation cannot release a protocol hold.
                return
            if creation is not None and (
                not bound
                or allocation.state["creation_binding"] != creation
                or allocation.state.get("creation_source") != creation_source
                or creation_source is None
                or creation_source["allocation"]["uid"] != allocation.uid
                or not rootdisk_dv_uid
                or rootdisk != expected_root
            ):
                raise PreparationConflict("Prepared completion authority changed.")
            dv, pvc = await self.store.dv(rootdisk), await self.store.pvc(rootdisk)
            if (
                not dv
                or not pvc
                or pvc.metadata.uid != pvc_uid
                or dv.get("status", {}).get("phase") != "Succeeded"
                or bound
                and (
                    dv["metadata"].get("uid") != rootdisk_dv_uid
                    or getattr(pvc.status, "phase", None) != "Bound"
                )
            ):
                return
            artifact = await self.store.get(allocation.state["artifact"])
            if artifact is None or artifact.uid != allocation.state["artifact_uid"]:
                raise PreparationConflict("Preparation clone source identity changed.")
            source = dv.get("spec", {}).get("source", {}).get("pvc", {})
            if (
                source.get("name") != artifact.state["disk"]
                or source.get("namespace", self.store.namespace) != self.store.namespace
            ):
                raise PreparationConflict(
                    "Workspace did not clone its admitted artifact."
                )
            await self.store.save(
                allocation,
                {
                    **allocation.state,
                    "phase": "Allocated",
                    "rootdisk": rootdisk,
                    "rootdisk_uid": pvc_uid,
                    **(
                        {
                            "creation_root": {
                                "name": rootdisk,
                                "dv_uid": rootdisk_dv_uid,
                                "pvc_uid": pvc_uid,
                            }
                        }
                        if bound
                        else {}
                    ),
                },
            )

    async def observe_workspace(self, owner_kind, owner_id, *, rootdisk, pvc_uid):
        for allocation in await self.store.records("allocation"):
            if (
                allocation.request["ownerKind"] == owner_kind
                and allocation.request["allocationId"] == owner_id
                and allocation.state["phase"] == "Cloning"
            ):
                await self.mark_allocated(
                    allocation.request, rootdisk=rootdisk, pvc_uid=pvc_uid
                )

    async def cancel(self, value):
        return (await self.cancel_with_receipt(value))["cancelled"]

    async def cancel_with_receipt(self, value):
        """Fence future source delivery and report durable non-issuance proof."""
        request = validate_request(value)
        async with self.lock:
            allocation = await self.store.ensure(
                allocation_name(request),
                "allocation",
                request,
                {"phase": "Pending", "workspace_source_issued": False},
            )
            cancelled = await self._abandon(allocation, "Cancelled", "BuildCancelled")
            never_issued = False
            if cancelled and not any(
                allocation.state.get(key) for key in ("rootdisk", "rootdisk_uid")
            ):
                issued = allocation.state.get("workspace_source_issued")
                if issued is False:
                    never_issued = True
                elif issued is None and allocation.state.get("artifact"):
                    # Older records did not track source issuance. A matching
                    # terminal builder with no success receipt could never have
                    # produced a Ready source. Unknown/missing artifacts and
                    # every previously successful build remain unproven.
                    artifact = await self.store.get(allocation.state["artifact"])
                    never_issued = bool(
                        artifact
                        and artifact.uid == allocation.state.get("artifact_uid")
                        and artifact.state.get("phase") == "Failed"
                        and artifact.state.get("terminal") is True
                        and "receipt" in artifact.state
                        and artifact.state.get("receipt") is None
                        and type(artifact.state.get("exit_code")) is int
                        and artifact.state.get("error")
                        in {"BuildFailed", "BuildCancelled"}
                    )
                    if never_issued:
                        await self.store.save(
                            allocation,
                            {**allocation.state, "workspace_source_issued": False},
                        )
            return {
                "cancelled": cancelled,
                "workspaceNeverIssued": never_issued,
            }

    async def _abandon(self, allocation, phase, error):
        if creation_held(allocation):
            return False
        await self.store.save(
            allocation, {**allocation.state, "phase": phase, "error": error}
        )
        if not allocation.state.get("artifact"):
            return True
        artifact = await self.store.get(allocation.state["artifact"])
        if artifact is None or artifact.uid != allocation.state["artifact_uid"]:
            return True
        holders = await self._holders(artifact)
        if holders or artifact.state["phase"] in {
            "Ready",
            "Failed",
            "Lost",
            "Deleting",
        }:
            return True
        if artifact.state["phase"] in {"Building", "Releasing"}:
            await self.store.save(
                artifact, {**artifact.state, "cancel_requested": True}
            )
            await self._stop_builder(artifact)
            return artifact.state["phase"] == "Failed"
        await self._fail(artifact, error)
        return True

    async def _stop_builder(self, artifact):
        if artifact.state["phase"] == "Building":
            await self._observe_builder(artifact)
            if artifact.state["phase"] == "Building":
                # A deadline asks kubelet to stop the container and retain its
                # terminal status on the Pod. Deleting first would lose proof.
                await self.store.call(
                    self.store.core.patch_namespaced_pod,
                    name=artifact.state["pod"],
                    body={
                        "metadata": {"uid": artifact.state["pod_uid"]},
                        "spec": {"activeDeadlineSeconds": 1},
                    },
                )
        if artifact.state["phase"] == "Releasing":
            await self._release_builder(artifact)

    async def _holders(self, artifact):
        return [
            a
            for a in await self.store.records("allocation", artifact_uid=artifact.uid)
            if a.state.get("artifact_uid") == artifact.uid
            and (a.state["phase"] in HELD or creation_held(a))
        ]

    async def _remove(self, artifact, *, allow_failed_holders=False):
        if artifact.state["phase"] not in {"Ready", "Failed", "Deleting"}:
            return False
        holders = await self._holders(artifact)
        if holders:
            if any(creation_held(allocation) for allocation in holders):
                return False
            if artifact.state["phase"] != "Failed" or not allow_failed_holders:
                return False
            for allocation in holders:
                await self.store.save(
                    allocation,
                    {
                        **allocation.state,
                        "phase": "Failed",
                        "error": artifact.state.get("error", "BuildFailed"),
                    },
                )
        retire_import = artifact.state["phase"] in {
            "Failed",
            "Deleting",
        } and not artifact.state.get("pod")
        if artifact.state.get("disk") and not artifact.state.get("pvc_uid"):
            pvc = await self.store.disk_identity(
                artifact.state["disk"], owner_uid=artifact.uid
            )
            await self.store.save(artifact, {**artifact.state, "pvc_uid": pvc})
        if artifact.state.get("disk") and not retire_import:
            await self.store.reap_disk_pods(
                artifact.state["disk"], artifact.state.get("pvc_uid")
            )
            if not await self.store.unused(artifact.state["disk"]):
                return False
        await self.store.save(artifact, {**artifact.state, "phase": "Deleting"})
        if artifact.state.get("disk") and not await self.store.delete_disk(
            artifact.state["disk"],
            owner_uid=artifact.uid,
            pvc_uid=artifact.state.get("pvc_uid"),
            dv_uid=artifact.state.get("dv_uid"),
            retire_import=retire_import,
        ):
            return False
        await self.store.delete_record(artifact)
        return True

    async def _remove_base(self, base, *, recovering=False):
        if base.state["phase"] not in {"Ready", "Failed", "Deleting"}:
            return False
        if not recovering and any(
            a.request["scope"] == base.request["scope"]
            and a.request["baseImage"] == base.request["image"]
            and a.state["phase"] in {"Queued", "Importing", "Cloning"}
            for a in await self.store.records("artifact")
        ):
            return False
        disk = base.state.get("disk")
        retire_import = base.state["phase"] in {"Failed", "Deleting"}
        if disk and not base.state.get("pvc_uid"):
            pvc = await self.store.disk_identity(disk, owner_uid=base.uid)
            await self.store.save(base, {**base.state, "pvc_uid": pvc})
        if disk and not retire_import:
            await self.store.reap_disk_pods(disk, base.state.get("pvc_uid"))
            if not await self.store.unused(disk):
                return False
        await self.store.save(base, {**base.state, "phase": "Deleting"})
        if disk and not await self.store.delete_disk(
            disk,
            owner_uid=base.uid,
            pvc_uid=base.state.get("pvc_uid"),
            dv_uid=base.state.get("dv_uid"),
            retire_import=retire_import,
        ):
            return False
        await self.store.delete_record(base)
        return True

    async def artifacts(self, scope):
        async with self.lock:
            return [
                self.view(a)
                for a in await self.store.records("artifact")
                if a.request["scope"] == scope
            ]

    @staticmethod
    def view(artifact):
        state = artifact.state
        return {
            "uid": artifact.uid,
            "scope": deepcopy(artifact.request["scope"]),
            "phase": state["phase"],
            "baseImage": artifact.request["baseImage"],
            "builderImage": artifact.request["builderImage"],
            "cacheKey": artifact.request["cacheKey"],
            "createdAt": artifact.created,
            "lastUsedAt": state.get("last_used"),
            "diskSha256": (state.get("receipt") or {}).get("diskSha256"),
            "error": state.get("error"),
        }

    async def delete_artifact(self, uid, scope):
        uid = str(UUID(uid))
        async with self.lock:
            matches = [
                a
                for a in await self.store.records("artifact")
                if a.uid == uid and a.request["scope"] == scope
            ]
            if not matches:
                return True
            return await self._remove(matches[0])

    async def reconcile(self):
        async with self.lock:
            if now() - self.last_allocation_sweep >= 60:
                self.last_allocation_sweep = now()
                for allocation in await self.store.records("allocation"):
                    expires = allocation.state.get(
                        "expires_at",
                        allocation.created + self.settings.wait_budget + 300,
                    )
                    if allocation.state["phase"] in HELD and (
                        now() > expires or not self.settings.enabled
                    ):
                        await self._abandon(
                            allocation,
                            "Failed",
                            "AllocationExpired"
                            if self.settings.enabled
                            else "PreparationDisabled",
                        )
                    elif (
                        allocation.state["phase"]
                        in {"Allocated", "Failed", "Cancelled"}
                        and (
                            "creation_binding" not in allocation.state
                            or allocation.state.get("workspace_source_issued") is False
                        )
                        and now() > expires + 7 * 86400
                    ):
                        await self.store.delete_record(allocation)
            artifacts = await self.store.records("artifact")
            # Reconcile active operations before considering completed cache
            # entries. A large cache must not starve newer builds.
            for artifact in sorted(
                artifacts,
                key=lambda x: (x.state["phase"] not in ACTIVE | {"Queued"}, x.created),
            ):
                try:
                    if artifact.state.get("cancel_requested") and artifact.state[
                        "phase"
                    ] in {"Building", "Releasing"}:
                        await self._stop_builder(artifact)
                    else:
                        await self._advance(artifact)
                    if (
                        self.settings.cache_ttl
                        and artifact.state["phase"] in {"Ready", "Failed", "Deleting"}
                        and now() - artifact.state.get("last_used", artifact.created)
                        > self.settings.cache_ttl
                    ):
                        await self._remove(artifact)
                except PreparationConflict:
                    await self._fail(artifact, "IdentityUnknown", lost=True)
            for base in await self.store.records("base"):
                try:
                    await self._reconcile_base(base)
                except PreparationConflict:
                    await self._fail(base, "IdentityUnknown", lost=True)
            if self.settings.cache_ttl:
                cached = {
                    (
                        json.dumps(a.request["scope"], sort_keys=True),
                        a.request["baseImage"],
                    )
                    for a in await self.store.records("artifact")
                } | {
                    (json.dumps(b.request["scope"], sort_keys=True), b.request["image"])
                    for b in await self.store.records("base")
                }
                for tag in await self.store.records("tag"):
                    if (
                        json.dumps(tag.request["scope"], sort_keys=True),
                        tag.state["image"],
                    ) not in cached and now() - tag.created > self.settings.cache_ttl:
                        await self.store.delete_record(tag)

    async def _reconcile_base(self, base):
        if base.state["phase"] == "Importing" and base.state.get("disk"):
            dv = await self.store.dv(base.state["disk"])
            if dv is None or dv["metadata"].get("uid") != base.state.get("dv_uid"):
                raise PreparationConflict("Base DataVolume identity changed")
            pvc_uid = await self.store.disk_identity(
                base.state["disk"],
                owner_uid=base.uid,
                expected=base.state.get("pvc_uid"),
            )
            phase = dv.get("status", {}).get("phase")
            state = {**base.state, "pvc_uid": pvc_uid}
            if phase == "Succeeded" and pvc_uid:
                state["phase"] = "Ready"
            elif (
                phase == "Failed" or now() - base.created > self.settings.import_timeout
            ):
                state.update(phase="Failed", error="BaseImportFailed")
            await self.store.save(base, state)
        if (
            self.settings.cache_ttl
            and now() - base.state.get("last_used", base.created)
            > self.settings.cache_ttl
        ):
            await self._remove_base(base)
