"""Exclusive, durable workspace attachments for manifest executions."""

from copy import deepcopy
import json
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.security.crypto import decrypt, encrypt
from orchestrator.services.generic_harness_runtime import GenericBindings, pinned_image
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_harness_egress import (
    HarnessEgressConfigurationError,
    validate_harness_egress,
)
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_workspace_runtime import (
    WorkspaceRuntimeIdentity,
    WorkspaceSSHMaterial,
    build_workspace_launch,
    generate_workspace_ssh_material,
    workspace_harness_bindings,
)
from shared.manifests.resolution import content_revision


def public_workspace_egress():
    """Installation policy: DNS and public internet, excluding internal ranges."""
    return (
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    },
                    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                }
            ],
            "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
        },
        {
            "to": [
                {
                    "ipBlock": {
                        "cidr": "0.0.0.0/0",
                        "except": [
                            "0.0.0.0/8",
                            "10.0.0.0/8",
                            "100.64.0.0/10",
                            "127.0.0.0/8",
                            "169.254.0.0/16",
                            "172.16.0.0/12",
                            "192.168.0.0/16",
                            "224.0.0.0/4",
                            "240.0.0.0/4",
                        ],
                    }
                }
            ]
        },
    )


class ManifestWorkspaceService:
    def __init__(
        self,
        db,
        runtime,
        *,
        namespace,
        default_image,
        storage_class_name=None,
        harness_namespace=None,
        vm_provisioner=None,
        workspace_egress="[]",
    ):
        self.db, self.runtime = db, runtime
        self._workspace_egress_error = None
        try:
            self._workspace_egress = validate_harness_egress(
                workspace_egress, setting="MANIFEST_WORKSPACE_EGRESS"
            )
        except HarnessEgressConfigurationError as exc:
            self._workspace_egress, self._workspace_egress_error = (), str(exc)
        self.vm_provisioner = vm_provisioner
        self.namespace, self.default_image = namespace, default_image
        self.storage_class_name = storage_class_name
        self.harness_namespace = harness_namespace or namespace

    def _require_workspace_egress(self) -> None:
        if self._workspace_egress_error:
            raise HTTPException(
                503,
                {
                    "code": "HostingConfigurationInvalid",
                    "message": self._workspace_egress_error,
                },
            )

    async def preparation_cache(self, user, scope, *, uid=None, request=None):
        """Inspect/evict scoped immutable artifacts through authenticated hosting."""
        from orchestrator.services.manifest_authority import ManifestAuthority
        import httpx

        if scope["kind"] not in {"Account", "Project"}:
            raise HTTPException(
                422, "Preparation caches belong to an Account or Project."
            )
        if scope["kind"] != "Account" or scope.get("name") not in {"me", "personal"}:
            try:
                UUID(str(scope.get("name")))
            except ValueError:
                raise HTTPException(
                    422, "Preparation scope requires a UUID or me."
                ) from None
        scope = await ManifestAuthority(self.db, user, request=request).scope(
            scope, write=uid is not None
        )
        if self.vm_provisioner is None:
            raise HTTPException(503, "VM preparation hosting is unavailable.")
        values = {"scope": {"kind": scope["kind"], "uid": str(UUID(scope["name"]))}}
        if uid is not None:
            values["uid"] = str(uid)
        try:
            return await self.vm_provisioner.preparation_operation(
                "delete" if uid else "list", values
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 409:
                raise HTTPException(
                    409,
                    "The artifact is still in use, building, or awaiting verified cleanup.",
                ) from None
            raise HTTPException(
                503, "Preparation cache operation is unavailable."
            ) from None
        except (ValueError, httpx.RequestError):
            raise HTTPException(
                503, "Authenticated preparation hosting is unavailable."
            ) from None

    async def read(self, instance_id, user, *, write=False, request=None):
        row = await self.db.fetchrow(
            "SELECT * FROM srw_workspace_instances WHERE id=$1", UUID(str(instance_id))
        )
        if not row:
            raise HTTPException(404, "Workspace instance does not exist.")
        authority = ManifestAuthority(self.db, user, request=request)
        if row["project_id"] is None and row["owner_id"] is None:
            # Only fenced, Released history can outlive an Account owner.
            # It does not become an ownerless shared workspace.
            if not user.get("is_admin"):
                await authority.deny(
                    "Deleted-account workspace history requires an administrator."
                )
            await authority.scope()
            return dict(row)
        scope = (
            {"kind": "Project", "name": str(row["project_id"])}
            if row["project_id"]
            else {"kind": "Account", "name": str(row["owner_id"])}
        )
        await authority.scope(scope, write=write)
        return dict(row)

    async def validate(self, workspace, user, *, request=None):
        self._require_workspace_egress()
        if "instanceRef" in workspace:
            try:
                row = await self.read(
                    workspace["instanceRef"]["uid"], user, write=True, request=request
                )
            except ValueError:
                raise HTTPException(
                    422, "Workspace instance references require a UUID."
                ) from None
            recipe = (
                json.loads(row["recipe"])
                if isinstance(row["recipe"], str)
                else row["recipe"]
            )
            if recipe.get("backend") == "vm":
                raise HTTPException(
                    422, "Retained VM instances require the SRW VM harness adapter."
                )
            if row["execution_id"] or row["status"] != "Detached":
                raise HTTPException(
                    409,
                    "Workspace instance is still attached or awaiting process fencing.",
                )
            return
        recipe = workspace["template"]["inline"]
        try:
            identity = WorkspaceRuntimeIdentity(str(uuid4()), 1, str(uuid4()))
            build_workspace_launch(
                identity,
                recipe,
                generate_workspace_ssh_material(),
                namespace=self.namespace,
                default_image=self.default_image,
                initialize=True,
                storage_class_name=self.storage_class_name,
            )
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None

    async def reserve(self, work_id, workspace, user):
        snapshot = await ManifestStore(self.db).execution("Job", work_id)
        if "instanceRef" in workspace:
            instance_id = UUID(workspace["instanceRef"]["uid"])
            row = await self.db.fetchrow(
                "SELECT * FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
                instance_id,
            )
            await self.read(instance_id, user, write=True)
            if row["execution_id"] or row["status"] != "Detached":
                raise HTTPException(
                    409, "Workspace instance already has an execution owner."
                )
            await self.db.execute(
                "UPDATE srw_workspace_instances SET execution_id=$2,status='Reserved',updated_at=now() WHERE id=$1",
                instance_id,
                snapshot["id"],
            )
        else:
            instance_id = uuid4()
            recipe = workspace["template"]["inline"]
            scope = snapshot["resolved"]["metadata"]["scope"]
            await self.db.execute(
                """INSERT INTO srw_workspace_instances(id,owner_id,project_id,recipe,revision,pvc_name,execution_id)
                VALUES($1,$2,$3,$4::jsonb,$5,$6,$7)""",
                instance_id,
                UUID(str(user["id"])),
                UUID(scope["name"]) if scope["kind"] == "Project" else None,
                json.dumps(recipe),
                content_revision(recipe),
                f"srw-ws-{instance_id.hex}",
                snapshot["id"],
            )
        await self.db.execute(
            "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
            snapshot["id"],
            instance_id,
        )

    async def _bound(self, snapshot):
        row = await self.db.fetchrow(
            """SELECT i.* FROM srw_workspace_instances i
            JOIN srw_execution_workspace_bindings b ON b.instance_id=i.id WHERE b.execution_id=$1""",
            snapshot["id"],
        )
        if not row:
            raise HTTPException(409, "Execution workspace reservation is absent.")
        result = dict(row)
        if isinstance(result["recipe"], str):
            result["recipe"] = json.loads(result["recipe"])
        return result

    async def attach(self, snapshot, attempt, bindings, user):
        row = await self._bound(snapshot)
        await self.read(row["id"], user, write=True)
        if row["execution_id"] != snapshot["id"]:
            raise HTTPException(409, "Workspace attachment authority changed.")
        if row["active_attempt"] != attempt.attempt:
            if row["pod_uid"] or row["status"] not in {"Reserved", "Detached"}:
                raise HTTPException(
                    409, "Prior workspace processes have not been fenced."
                )
            material = generate_workspace_ssh_material()
            identity = WorkspaceRuntimeIdentity(
                str(row["id"]), row["generation"] + 1, str(snapshot["id"])
            )
            await self.db.execute(
                """UPDATE srw_workspace_instances SET generation=$2,active_attempt=$3,pod_name=$4,
                ssh_ciphertext=$5,status='Preparing',updated_at=now() WHERE id=$1 AND execution_id=$6""",
                row["id"],
                identity.generation,
                attempt.attempt,
                identity.pod_name,
                encrypt(material.to_json()),
                snapshot["id"],
            )
            row = await self._bound(snapshot)
        identity = WorkspaceRuntimeIdentity(
            str(row["id"]), row["generation"], str(snapshot["id"])
        )
        material = WorkspaceSSHMaterial.from_json(decrypt(row["ssh_ciphertext"]))
        ingress = (
            {
                "from": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {
                                "kubernetes.io/metadata.name": self.harness_namespace
                            }
                        },
                        "podSelector": {"matchLabels": attempt.labels},
                    }
                ],
                "ports": [{"protocol": "TCP", "port": 30022}],
            },
        )
        recipe = deepcopy(row["recipe"])
        if row["generation"] > 1:
            environment = recipe.setdefault("environment", {})
            environment["image"] = pinned_image(
                row["image_id"], environment.get("image", self.default_image)
            )
        plan = build_workspace_launch(
            identity,
            recipe,
            material,
            namespace=self.namespace,
            default_image=self.default_image,
            initialize=not row["initialized"],
            storage_class_name=self.storage_class_name,
            ingress=ingress,
            egress=(*public_workspace_egress(), *deepcopy(self._workspace_egress)),
        )
        pvc_uid = await self.runtime.ensure_volume(
            plan, expected_pvc_uid=row["pvc_uid"]
        )
        await self.db.execute(
            "UPDATE srw_workspace_instances SET pvc_uid=$2 WHERE id=$1 AND execution_id=$3",
            row["id"],
            pvc_uid,
            snapshot["id"],
        )
        observed = await self.runtime.observe(identity, expected_pod_uid=row["pod_uid"])
        if observed.pod_absent:
            if row["pod_uid"]:
                raise HTTPException(
                    409,
                    "Workspace pod disappeared without terminal evidence; automatic replacement is fenced.",
                )
            observed = await self.runtime.launch(plan)
        if observed.phase == "Replaced":
            raise HTTPException(409, "Workspace pod identity was replaced.")
        if observed.pod_uid:
            await self.db.execute(
                "UPDATE srw_workspace_instances SET pod_uid=$2,status='Attached',updated_at=now() WHERE id=$1 AND execution_id=$3",
                row["id"],
                observed.pod_uid,
                snapshot["id"],
            )
        if observed.image_id and not row["image_id"]:
            await self.db.execute(
                "UPDATE srw_workspace_instances SET image_id=$2 WHERE id=$1 AND execution_id=$3 AND image_id IS NULL",
                row["id"],
                observed.image_id,
                snapshot["id"],
            )
        if observed.initialization_succeeded and not row["initialized"]:
            await self.db.execute(
                "UPDATE srw_workspace_instances SET initialized=true WHERE id=$1 AND execution_id=$2",
                row["id"],
                snapshot["id"],
            )
        if (
            not observed.pod_ip
            or observed.readiness is not True
            or not observed.initialization_succeeded
        ):
            return None
        workspace_bindings = workspace_harness_bindings(
            identity, material, pod_ip=observed.pod_ip, namespace=self.namespace
        )
        return GenericBindings(
            secret_env=bindings.secret_env,
            environment=bindings.environment,
            files=(*bindings.files, *workspace_bindings.files),
            descriptor={
                **deepcopy(bindings.descriptor or {}),
                **workspace_bindings.descriptor,
            },
            egress=(*bindings.egress, *workspace_bindings.egress),
        )

    async def detach(self, snapshot, attempt, *, final):
        row = await self._bound(snapshot)
        if row["execution_id"] is None and row["status"] in {"Detached", "Released"}:
            return True
        if row["execution_id"] != snapshot["id"]:
            return False
        identity = WorkspaceRuntimeIdentity(
            str(row["id"]), max(1, row["generation"]), str(snapshot["id"])
        )
        observed = await self.runtime.observe(identity, expected_pod_uid=row["pod_uid"])
        if observed.phase == "Replaced":
            return False
        if observed.pod_uid:
            # Record exact identity even when a successful create response was
            # lost before publishing its UID in the instance record.
            await self.db.execute(
                "UPDATE srw_workspace_instances SET pod_uid=$2,updated_at=now() WHERE id=$1",
                row["id"],
                observed.pod_uid,
            )
            if not observed.containers_terminal:
                await self.runtime.cancel(identity, expected_pod_uid=observed.pod_uid)
                return False
            await self.db.execute(
                "UPDATE srw_workspace_instances SET status='Terminated' WHERE id=$1",
                row["id"],
            )
            if not await self.runtime.cleanup(
                identity, expected_pod_uid=observed.pod_uid
            ):
                return False
        elif row["pod_uid"] and row["status"] != "Terminated":
            return False
        elif row["pod_uid"] and not await self.runtime.cleanup(
            identity, expected_pod_uid=row["pod_uid"]
        ):
            return False
        status = "Detached"
        if (
            final
            and row["recipe"].get("retention", "Delete") == "Delete"
            and row["pvc_uid"]
        ):
            if not await self.runtime.delete_volume(
                identity, expected_pvc_uid=row["pvc_uid"]
            ):
                return False
            status = "Released"
        await self.db.execute(
            """UPDATE srw_workspace_instances SET status=$2,pod_name=NULL,pod_uid=NULL,
            active_attempt=NULL,ssh_ciphertext=NULL,execution_id=CASE WHEN $3 THEN NULL ELSE execution_id END,updated_at=now() WHERE id=$1 AND execution_id=$4""",
            row["id"],
            status,
            final,
            snapshot["id"],
        )
        return True

    async def reconcile_vm_detach(self):
        if self.vm_provisioner is not None:
            from orchestrator.services.retained_vm_workspaces import reconcile_detached

            await reconcile_detached(self.db, self.vm_provisioner)

    async def view(self, instance_id, user, *, request=None):
        row = await self.read(instance_id, user, request=request)
        recipe = (
            json.loads(row["recipe"])
            if isinstance(row["recipe"], str)
            else row["recipe"]
        )
        return {
            "uid": str(row["id"]),
            "generation": row["generation"],
            "status": row["status"],
            "retention": recipe.get("retention", "Delete"),
            "initialized": row["initialized"],
            "executionId": str(row["execution_id"]) if row["execution_id"] else None,
        }

    async def delete(self, instance_id, user, *, expected_generation, request=None):
        initial = await self.read(instance_id, user, write=True, request=request)
        recipe = (
            json.loads(initial["recipe"])
            if isinstance(initial["recipe"], str)
            else initial["recipe"]
        )
        if recipe.get("backend") == "vm":
            return await self._delete_vm(
                instance_id,
                user,
                expected_generation=expected_generation,
                request=request,
            )
        async with self.db.transaction_scope():
            await ManifestStore(self.db).lock_catalog()
            await self.db.fetchrow(
                "SELECT id FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
                UUID(str(instance_id)),
            )
            row = await self.read(instance_id, user, write=True, request=request)
            if row["generation"] != expected_generation:
                raise HTTPException(
                    409, "Workspace generation changed; read its current status."
                )
            if (
                row["execution_id"]
                or row["pod_uid"]
                or row["status"] not in {"Detached", "Released"}
            ):
                raise HTTPException(
                    409,
                    "Workspace processes must be fenced and ownership released before deletion.",
                )
            if row["status"] == "Released":
                return {"deleted": True, "uid": str(instance_id)}
            identity = WorkspaceRuntimeIdentity(
                str(instance_id), max(1, row["generation"]), str(uuid4())
            )
            if row["pvc_uid"] and not await self.runtime.delete_volume(
                identity, expected_pvc_uid=row["pvc_uid"]
            ):
                return {"deleted": False, "uid": str(instance_id), "status": "Deleting"}
            await self.db.execute(
                "UPDATE srw_workspace_instances SET status='Released',updated_at=now() WHERE id=$1",
                row["id"],
            )
            return {"deleted": True, "uid": str(instance_id)}

    async def _delete_vm(self, instance_id, user, *, expected_generation, request=None):
        from orchestrator.services.retained_vm_workspaces import object_value

        if self.vm_provisioner is None:
            raise HTTPException(503, "VM workspace hosting is unavailable.")
        async with self.db.transaction_scope():
            await ManifestStore(self.db).lock_catalog()
            await self.db.fetchrow(
                "SELECT id FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
                UUID(str(instance_id)),
            )
            row = await self.read(instance_id, user, write=True, request=request)
            if row["generation"] != expected_generation:
                raise HTTPException(
                    409, "Workspace generation changed; read its current status."
                )
            if row["status"] == "Released":
                return {"deleted": True, "uid": str(instance_id)}
            if row["execution_id"]:
                terminal_unallocated = not row["pvc_uid"] and await self.db.fetchval(
                    "SELECT j.status IN ('completed','failed','cancelled') FROM srw_execution_specs s JOIN jobs j ON s.work_kind='Job' AND s.work_id=j.id WHERE s.id=$1",
                    row["execution_id"],
                )
                if not terminal_unallocated:
                    raise HTTPException(
                        409,
                        "Workspace processes must be fenced and ownership released before deletion.",
                    )
            elif row["status"] not in {"Detached", "Deleting"}:
                raise HTTPException(409, "Workspace is not ready for deletion.")
            binding = deepcopy(object_value(row["backend_state"])["storage"])
            binding["pvc_uid"] = row["pvc_uid"]
            # Persist the deletion intent before external I/O. A lost response
            # or rollback must never make a tombstoned disk available to a Job.
            await self.db.execute(
                "UPDATE srw_workspace_instances SET status='Deleting',updated_at=now() WHERE id=$1",
                row["id"],
            )
        if not await self.vm_provisioner.release_workspace_storage(binding):
            return {"deleted": False, "uid": str(instance_id), "status": "Deleting"}
        await self.db.execute(
            "UPDATE srw_workspace_instances SET status='Released',execution_id=NULL,updated_at=now() WHERE id=$1 AND generation=$2 AND status='Deleting'",
            UUID(str(instance_id)),
            expected_generation,
        )
        return {"deleted": True, "uid": str(instance_id)}
