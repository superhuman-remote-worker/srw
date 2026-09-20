"""One-shot Kubernetes effects under the existing durable creation admission.

Observer expiry is never permission to repeat an issued API request. All methods
release database transactions before controller I/O; unknown results stay held.
"""

import asyncio
from copy import deepcopy
import json
from uuid import UUID, uuid5

from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException

from shared.vm_creation_retry import canonical_request_digest
from shared.vm_creation_issuance import (
    CREATION_INTENT_ANNOTATION,
    CREATION_SIGNATURE_ANNOTATION,
    EFFECT_NONCE_ANNOTATION,
    REQUEST_ANNOTATION,
    EFFECT_KINDS,
    seal_creation_carrier,
    verify_creation_carrier,
    public_effect_observation,
)
from shared.vm_workspace_storage import storage_name, storage_labels
from vm_controller.creation_configuration import resolve_creation_configuration


class CreationUnproven(ValueError):
    pass


def document(value):
    return (
        deepcopy(value)
        if isinstance(value, dict)
        else ApiClient().sanitize_for_serialization(value)
    )


def carrier_record(lease, *, secret):
    """Inventory adapter only; source-specific authority never uses legacy resume."""
    lease = document(lease)
    values = verify_creation_carrier(lease, secret=secret)
    return {
        **values,
        "owner_kind": "job",
        "owner_id": values["job_id"],
        "name": values["object_name"],
        "carrier_name": lease["metadata"]["name"],
        "carrier_uid": lease["metadata"]["uid"],
        "carrier_sealed": True,
        "creation_lease": lease,
    }


class CreationActuator:
    def __init__(self, controller):
        from vm_controller import controller as settings

        self.controller = controller
        self.settings = settings
        self.namespace = settings.VM_NAMESPACE
        self.secret = settings.LIFECYCLE_HMAC_SECRET

    async def authority(self, method, **payload):
        return await self.controller._workspace_cleanup_authority_request(
            "/api/internal/vm-creation-retries/" + method,
            payload,
            operation="creation_retry_" + method.replace("-", "_"),
        )

    async def read(self, kind, name):
        ctrl = self.controller
        kwargs = {"namespace": self.namespace, "name": name}
        if kind in ("rootdisk", "vm"):
            method = ctrl.k8s_client.get_namespaced_custom_object
            kwargs.update(
                group="cdi.kubevirt.io" if kind == "rootdisk" else "kubevirt.io",
                version="v1beta1" if kind == "rootdisk" else "v1",
                plural="datavolumes" if kind == "rootdisk" else "virtualmachines",
            )
        else:
            method = {
                "pvc": ctrl.core_api.read_namespaced_persistent_volume_claim,
                "cloud_init": ctrl.core_api.read_namespaced_secret,
                "lease": ctrl.coordination_api.read_namespaced_lease,
            }[kind]
        try:
            return document(await asyncio.to_thread(method, **kwargs))
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def seal(self, values, lease):
        meta = lease["metadata"]
        return seal_creation_carrier(
            values,
            namespace=self.namespace,
            uid=meta["uid"],
            resource_version=meta["resourceVersion"],
            secret=self.secret,
        )

    async def publish(self, values, prior=None):
        """Create/seal or advance one exact Lease UID with resourceVersion CAS."""
        name = "srw-cleanup-" + UUID(values["admission_id"]).hex
        lease = await self.read("lease", name)
        if lease is None:
            if prior is not None:
                raise CreationUnproven("creation_carrier_missing")
            body = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "labels": {"srw.io/vm-workspace-cleanup-carrier": "true"},
                    "annotations": {
                        CREATION_INTENT_ANNOTATION: json.dumps(
                            values, sort_keys=True, separators=(",", ":")
                        )
                    },
                },
                "spec": {"holderIdentity": values["admission_id"]},
            }
            try:
                lease = document(
                    await asyncio.to_thread(
                        self.controller.coordination_api.create_namespaced_lease,
                        namespace=self.namespace,
                        body=body,
                    )
                )
            except Exception:
                lease = await self.read("lease", name)
                if lease is None:
                    raise
        metadata = lease["metadata"]
        if metadata.get("deletionTimestamp") is not None:
            raise CreationUnproven("creation_carrier_deleting")
        annotations = metadata.get("annotations") or {}
        if annotations.get(CREATION_SIGNATURE_ANNOTATION):
            current = verify_creation_carrier(lease, secret=self.secret)
            if current == values:
                return lease
            if current != prior:
                raise CreationUnproven("creation_carrier_changed")
        else:
            current = json.loads(annotations.get(CREATION_INTENT_ANNOTATION, "null"))
            # Concurrent first publishers use one deterministic stage nonce.
            if (
                current != values
                or lease.get("spec", {}).get("holderIdentity") != values["admission_id"]
            ):
                raise CreationUnproven("creation_carrier_changed")
        body = self.seal(values, lease)
        try:
            result = document(
                await asyncio.to_thread(
                    self.controller.coordination_api.replace_namespaced_lease,
                    name=name,
                    namespace=self.namespace,
                    body=body,
                )
            )
        except Exception:
            result = await self.read("lease", name)
            if result is None:
                raise
        if (
            verify_creation_carrier(result, secret=self.secret) != values
            or result["metadata"]["uid"] != metadata["uid"]
        ):
            raise CreationUnproven("creation_carrier_changed")
        return result

    async def disk(self, row, *, expected=None):
        request = row["request"]
        binding = request.get("workspace_storage")
        name = (
            storage_name(binding)
            if binding
            else "agent-vm-" + row["job_id"] + "-rootdisk"
        )
        dv, pvc = await self.read("rootdisk", name), await self.read("pvc", name)
        for obj in (dv, pvc):
            if obj is None:
                continue
            metadata = obj.get("metadata", {})
            if (
                metadata.get("name") != name
                or metadata.get("namespace") != self.namespace
                or str(UUID(metadata.get("uid", ""))) != metadata.get("uid")
            ):
                raise CreationUnproven("retained_disk_changed")
        if dv is not None:
            meta = dv.get("metadata", {})
            labels = meta.get("labels", {})
            if (
                meta.get("deletionTimestamp")
                or dv.get("status", {}).get("phase") == "Failed"
                or labels.get("srw.io/owner-kind") != "job"
                or labels.get("srw.io/owner-id") != row["job_id"]
            ):
                raise CreationUnproven("retained_disk_changed")
        expected_pvc = (expected or {}).get("pvc_uid") or row["expected_pvc_uid"]
        if expected_pvc:
            if dv is None or pvc is None or pvc["metadata"]["uid"] != expected_pvc:
                raise CreationUnproven("retained_disk_changed")
            if expected and dv["metadata"]["uid"] != expected["uid"]:
                raise CreationUnproven("retained_disk_changed")
        if pvc is not None:
            meta = pvc.get("metadata", {})
            labels = meta.get("labels", {})
            if (
                meta.get("deletionTimestamp")
                or labels.get("srw.io/owner-kind") != "job"
                or labels.get("srw.io/owner-id") != row["job_id"]
            ):
                raise CreationUnproven("retained_disk_changed")
            if dv is None or not any(
                ref.get("kind") == "DataVolume"
                and ref.get("uid") == dv["metadata"]["uid"]
                for ref in meta.get("ownerReferences", [])
            ):
                raise CreationUnproven("retained_disk_changed")
            if self.controller._pvc_is_recovery_pinned(
                await self.controller._active_recovery_pins(), meta["uid"]
            ):
                raise CreationUnproven("workspace_recovery_held")
        if binding:
            from shared.vm_workspace_storage import (
                WORKSPACE_LABEL,
                GENERATION_LABEL,
                EXECUTION_LABEL,
            )

            lease = await self.read("lease", name)
            labels = (lease or {}).get("metadata", {}).get("labels", {})
            annotations = (lease or {}).get("metadata", {}).get("annotations", {})
            if (
                not dv
                or not pvc
                or not lease
                or lease["metadata"].get("deletionTimestamp")
                or labels.get(WORKSPACE_LABEL) != binding["uid"]
                or labels.get(GENERATION_LABEL) != str(binding["generation"])
                or labels.get(EXECUTION_LABEL) != row["job_id"]
                or annotations.get("srw.io/released") == "true"
                or annotations.get("srw.io/detached") == "true"
                or any(
                    obj["metadata"].get("labels", {}).get(WORKSPACE_LABEL)
                    != binding["uid"]
                    for obj in (dv, pvc)
                )
            ):
                raise CreationUnproven("workspace_attachment_unproven")
        return name, dv, pvc

    async def observation(self, row, effect, lease):
        values = effect["carrier_intent"]
        kind = values["effect_kind"]
        disk_evidence = next(
            (
                e["evidence"]
                for e in row["effects"]
                if e["state"] == "observed"
                and e["carrier_intent"]["effect_kind"] == "rootdisk"
            ),
            None,
        )
        _, dv, pvc = await self.disk(row, expected=disk_evidence)
        obj = dv if kind == "rootdisk" else await self.read(kind, values["object_name"])
        if obj is None or (kind == "rootdisk" and pvc is None):
            return None
        # Only public metadata/reference evidence crosses the authority boundary.
        obj.pop("data", None)
        obj.pop("stringData", None)
        result = {"outcome": "observed", "object": obj}
        if kind == "rootdisk":
            result["pvc"] = pvc
        secret_evidence = next(
            (
                e["evidence"]
                for e in row["effects"]
                if e["state"] == "observed"
                and e["carrier_intent"]["effect_kind"] == "cloud_init"
            ),
            None,
        )
        public_effect_observation(
            values,
            self.seal(values, lease),
            result,
            rootdisk=disk_evidence,
            cloud_init=secret_evidence,
        )
        return result

    async def exact_previous(self, row, lease):
        observations = {}
        for effect in row["effects"]:
            if effect["state"] != "observed":
                continue
            observation = await self.observation(row, effect, lease)
            if observation is None:
                raise CreationUnproven("creation_observed_object_missing")
            prior = {
                e["carrier_intent"]["effect_kind"]: e["evidence"]
                for e in row["effects"]
                if e["state"] == "observed"
            }
            actual = public_effect_observation(
                effect["carrier_intent"],
                self.seal(effect["carrier_intent"], lease),
                observation,
                rootdisk=prior.get("rootdisk"),
                cloud_init=prior.get("cloud_init"),
            )
            if actual != effect["evidence"]:
                raise CreationUnproven("creation_observed_object_changed")
            observations[effect["carrier_intent"]["effect_kind"]] = observation
        return observations

    def values(self, row, reservation, kind, dv, pvc, previous):
        prior = {
            e["carrier_intent"]["effect_kind"]: e["evidence"]
            for e in row["effects"]
            if e["state"] == "observed"
        }
        nonce = uuid5(
            UUID(previous["effect_nonce"] if previous else row["request_id"]), kind
        )
        disk = prior.get("rootdisk")
        return {
            "version": 1,
            "source": "controller_vm_create",
            "admission_id": str(reservation["admission_id"]),
            "reservation_request_id": str(reservation["request_id"]),
            "intent_digest": reservation["intent_digest"],
            "retry_request_id": row["request_id"],
            "job_id": row["job_id"],
            "provision_generation": row["provision_generation"],
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
            "expected_pvc_uid": row["expected_pvc_uid"],
            "retained_dv_uid": dv["metadata"]["uid"]
            if row["expected_pvc_uid"]
            else None,
            "current_dv_uid": disk["uid"]
            if disk
            else dv["metadata"]["uid"]
            if row["expected_pvc_uid"]
            else None,
            "current_pvc_uid": disk["pvc_uid"] if disk else row["expected_pvc_uid"],
            "current_secret_uid": prior["cloud_init"]["uid"] if kind == "vm" else None,
            "effect_kind": kind,
            "effect_nonce": str(nonce),
            "object_name": (
                storage_name(row["request"]["workspace_storage"])
                if row["request"].get("workspace_storage")
                else "agent-vm-" + row["job_id"] + "-rootdisk"
            )
            if kind == "rootdisk"
            else "agent-vm-"
            + row["job_id"]
            + ("-cloudinit" if kind == "cloud_init" else ""),
        }

    async def body(self, row, values):
        request = row["request"]
        key = ""
        if (
            values["effect_kind"] == "cloud_init"
            and self.controller.headscale.is_available
        ):
            key = await self.controller.headscale.create_auth_key(row["job_id"])
            if not key:
                raise CreationUnproven("creation_headscale_unavailable")
        manifest = self.controller.render_template(request, key)
        user_data = manifest.pop("_srwCloudInitUserData", None)
        fingerprint = manifest.pop("_srwSSHHostKeyFingerprint", None)
        templates = manifest["spec"].pop("dataVolumeTemplates", [])
        if len(templates) != 1:
            raise CreationUnproven("creation_rootdisk_template_unproven")
        binding = request.get("workspace_storage")
        name = (
            storage_name(binding)
            if binding
            else "agent-vm-" + row["job_id"] + "-rootdisk"
        )
        for volume in manifest["spec"]["template"]["spec"].get("volumes", []):
            if (
                volume.get("dataVolume", {}).get("name")
                == templates[0]["metadata"]["name"]
            ):
                volume["dataVolume"]["name"] = name
        if binding:
            for metadata in (
                manifest["metadata"],
                manifest["spec"]["template"].setdefault("metadata", {}),
            ):
                metadata.setdefault("labels", {}).update(
                    storage_labels(binding, row["job_id"])
                )
        if values["effect_kind"] == "rootdisk":
            source = templates[0]["spec"].get("source", {}).get("pvc")
            if source is not None:
                source.setdefault("namespace", self.namespace)
            result = {
                "apiVersion": "cdi.kubevirt.io/v1beta1",
                "kind": "DataVolume",
                "metadata": {"name": name},
                "spec": templates[0]["spec"],
            }
            result["metadata"]["labels"] = {
                "srw.io/rootdisk": "true",
                "job-id": row["job_id"],
            }
        elif values["effect_kind"] == "cloud_init":
            if not user_data or not self.settings._ssh_host_key_fingerprint(
                fingerprint
            ):
                raise CreationUnproven("creation_secret_unproven")
            result = {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": values["object_name"],
                    "annotations": {"srw.io/ssh-host-key-fingerprint": fingerprint},
                },
                "type": "Opaque",
                "stringData": {"userdata": user_data},
            }
        else:
            result = manifest
        metadata = result["metadata"]
        if metadata["name"] != values["object_name"]:
            raise CreationUnproven("creation_object_name_changed")
        metadata["namespace"] = self.namespace
        metadata.setdefault("labels", {}).update(
            {"srw.io/owner-kind": "job", "srw.io/owner-id": row["job_id"]}
        )
        metadata.setdefault("annotations", {}).update(
            {
                EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                REQUEST_ANNOTATION: row["request_id"],
                "srw.io/provision-generation": row["provision_generation"],
            }
        )
        return result

    async def create_object(self, kind, body):
        kwargs = {"namespace": self.namespace, "body": body}
        if kind == "cloud_init":
            method = self.controller.core_api.create_namespaced_secret
        else:
            method = self.controller.k8s_client.create_namespaced_custom_object
            kwargs.update(
                group="cdi.kubevirt.io" if kind == "rootdisk" else "kubevirt.io",
                version="v1beta1" if kind == "rootdisk" else "v1",
                plural="datavolumes" if kind == "rootdisk" else "virtualmachines",
            )
        return await asyncio.to_thread(method, **kwargs)

    async def require_vm_absent(self, row):
        # A same-name VM without this ledger's issued VM effect is not an
        # adoption candidate, even when its generation matches. Creating its
        # missing disk or Secret could make that unproven guest executable.
        if await self.read("vm", "agent-vm-" + row["job_id"]) is not None:
            raise CreationUnproven("creation_existing_vm_unproven")

    async def run(self, payload):
        base = {
            "job_id": payload.get("job_id"),
            "provision_generation": payload.get("provision_generation"),
        }
        try:
            return await self._run(payload)
        except (CreationUnproven, ValueError, KeyError, TypeError):
            return {
                **base,
                "status": "creation_attention",
                "reason": "creation_evidence_unproven",
            }
        except Exception:
            # No API response or credential material is returned or logged.
            return {
                **base,
                "status": "creation_pending",
                "reason": "creation_observation_pending",
            }

    async def _run(self, payload):
        request = dict(payload)
        envelope = request.pop("creation_retry")
        if (
            not isinstance(envelope, dict)
            or set(envelope)
            != {
                "version",
                "request_id",
                "claim_token",
                "request_digest",
                "controller_configuration_digest",
            }
            or type(envelope["version"]) is not int
            or envelope["version"] != 1
            or self.secret is None
        ):
            raise CreationUnproven("creation_protocol_unproven")
        for key in ("request_id", "claim_token"):
            if str(UUID(envelope[key])) != envelope[key]:
                raise CreationUnproven("creation_protocol_unproven")
        if canonical_request_digest(request) != envelope["request_digest"]:
            raise CreationUnproven("creation_request_changed")
        base = {
            "job_id": request["job_id"],
            "provision_generation": request["provision_generation"],
        }
        pending = {
            **base,
            "status": "creation_pending",
            "reason": "creation_observation_pending",
        }
        for _ in range(7):
            row = await self.authority("inspect", request_id=envelope["request_id"])
            if (
                any(
                    row.get(key) != value
                    for key, value in {
                        **base,
                        "request_id": envelope["request_id"],
                        "request_digest": envelope["request_digest"],
                        "controller_configuration_digest": envelope[
                            "controller_configuration_digest"
                        ],
                    }.items()
                )
                or row["request"] != request
            ):
                raise CreationUnproven("creation_request_changed")
            effects = row["effects"]
            latest = effects[-1] if effects else None
            lease = None
            if row.get("creation_carrier_uid"):
                lease = await self.read(
                    "lease", "srw-cleanup-" + UUID(row["creation_admission_id"]).hex
                )
                if not lease or lease["metadata"]["uid"] != row["creation_carrier_uid"]:
                    raise CreationUnproven("creation_carrier_changed")
                verify_creation_carrier(lease, secret=self.secret)
                observations = await self.exact_previous(row, lease)
                if latest and latest["state"] == "issued":
                    observation = await self.observation(row, latest, lease)
                    if observation is None:
                        return pending
                    result = await self.authority(
                        "observe-effect",
                        request_id=row["request_id"],
                        carrier=self.seal(latest["carrier_intent"], lease),
                        observation=observation,
                    )
                    if result.get("recorded") is not True:
                        return pending
                    continue
                if (
                    latest
                    and latest["state"] == "observed"
                    and latest["carrier_intent"]["effect_kind"] == "vm"
                ):
                    result = await self.authority(
                        "settle-adopted",
                        request_id=row["request_id"],
                        carrier=self.seal(latest["carrier_intent"], lease),
                        observations=observations,
                    )
                    if result.get("settled") is not True:
                        return pending
                    vm = latest["evidence"]
                    return {
                        **base,
                        "status": "created",
                        "vm_name": vm["name"],
                        "namespace": self.namespace,
                        "vm_uid": vm["uid"],
                        "rootdisk_pvc_uid": vm["pvc_uid"],
                        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
                    }
            if row["state"] in {
                "succeeded",
                "settled",
                "cancel_requested",
                "attention",
            }:
                raise CreationUnproven("creation_source_not_active")
            # Already-issued VM observation/adoption returned above. All paths
            # below would grant a fresh effect and require authoritative absence.
            await self.require_vm_absent(row)
            resolved = resolve_creation_configuration(self.controller, request)
            if (
                resolved["request_digest"] != row["request_digest"]
                or resolved["controller_configuration_digest"]
                != row["controller_configuration_digest"]
            ):
                raise CreationUnproven("creation_configuration_changed")
            # Prepared/golden source pinning needs its own proven source result;
            # never substitute a registry import for requested preparation.
            if (
                request.get("preparation") is not None
                or self.settings.VM_GOLDEN_IMAGE_ENABLED
            ):
                raise CreationUnproven("creation_source_preparation_unproven")
            if await self.controller._capacity_wait("agent-vm-" + row["job_id"]):
                return {**pending, "reason": "capacity_wait"}
            _, dv, pvc = await self.disk(row)
            if (
                latest is None
                and row["expected_pvc_uid"] is None
                and (dv is not None or pvc is not None)
            ):
                raise CreationUnproven("creation_existing_disk_unproven")
            observed = {
                key: row[key]
                for key in (
                    "job_id",
                    "provision_generation",
                    "request_digest",
                    "controller_configuration_digest",
                    "expected_pvc_uid",
                )
            }
            reservation = await self.authority(
                "authorize",
                request_id=row["request_id"],
                claim_token=envelope["claim_token"],
                observed=observed,
            )
            if reservation.get("allowed") is not True:
                return pending
            previous = latest["carrier_intent"] if latest else None
            kind = (
                (
                    previous["effect_kind"]
                    if latest["state"] == "rejected"
                    else EFFECT_KINDS[EFFECT_KINDS.index(previous["effect_kind"]) + 1]
                )
                if latest
                else "rootdisk"
            )
            values = self.values(row, reservation, kind, dv, pvc, previous)
            carrier = await self.publish(values, prior=previous)
            if lease and carrier["metadata"]["uid"] != lease["metadata"]["uid"]:
                raise CreationUnproven("creation_carrier_changed")
            await self.exact_previous(row, carrier)
            await self.disk(row)
            await self.require_vm_absent(row)
            body = (
                None
                if kind == "rootdisk" and row["expected_pvc_uid"]
                else await self.body(row, values)
            )
            grant = await self.authority(
                "begin-effect",
                request_id=row["request_id"],
                claim_token=envelope["claim_token"],
                carrier=carrier,
            )
            if grant.get("actuation_allowed") is not True:
                return pending
            # The returned CAS grants only this one API call. Any subsequent
            # refusal/transport loss remains conservatively issued-unknown.
            current = await self.read("lease", carrier["metadata"]["name"])
            if (
                not current
                or current["metadata"]["uid"] != carrier["metadata"]["uid"]
                or verify_creation_carrier(current, secret=self.secret) != values
            ):
                raise CreationUnproven("creation_carrier_changed")
            await self.exact_previous(row, current)
            await self.disk(row)
            await self.require_vm_absent(row)
            if body is not None:
                try:
                    await self.create_object(kind, body)
                except ApiException as exc:
                    try:
                        status = json.loads(exc.body or "null")
                        if (
                            not isinstance(status, dict)
                            or status.get("code") != exc.status
                        ):
                            return pending
                        rejection = {"outcome": "rejected", "api_status": status}
                        public_effect_observation(values, carrier, rejection)
                    except (ValueError, TypeError):
                        return pending
                    await self.authority(
                        "observe-effect",
                        request_id=row["request_id"],
                        carrier=carrier,
                        observation=rejection,
                    )
                    return pending
                except Exception:
                    return pending
            # Inspect again, then record exact read-back of this issued nonce.
        return pending


async def reconcile_creation_carrier(controller, carrier):
    """Restart observer: never grants or performs another Kubernetes effect."""
    actuator = CreationActuator(controller)
    lease = carrier["creation_lease"]
    values = verify_creation_carrier(lease, secret=actuator.secret)
    current = await actuator.read("lease", lease["metadata"]["name"])
    if not current or current["metadata"]["uid"] != lease["metadata"]["uid"]:
        raise CreationUnproven("creation_carrier_changed")
    verify_creation_carrier(current, secret=actuator.secret)
    row = await actuator.authority("inspect", request_id=values["retry_request_id"])
    if row.get("creation_carrier_uid") != current["metadata"]["uid"]:
        raise CreationUnproven("creation_carrier_changed")
    observations = await actuator.exact_previous(row, current)
    latest = row["effects"][-1] if row["effects"] else None
    if latest and latest["state"] == "issued":
        observation = await actuator.observation(row, latest, current)
        if observation is not None:
            await actuator.authority(
                "observe-effect",
                request_id=row["request_id"],
                carrier=actuator.seal(latest["carrier_intent"], current),
                observation=observation,
            )
    elif (
        latest
        and latest["state"] == "observed"
        and latest["carrier_intent"]["effect_kind"] == "vm"
    ):
        await actuator.authority(
            "settle-adopted",
            request_id=row["request_id"],
            carrier=actuator.seal(latest["carrier_intent"], current),
            observations=observations,
        )
    return False  # Preserve the carrier/tombstone even after exact adoption.
