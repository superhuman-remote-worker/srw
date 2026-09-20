"""Native manifest admission and durable observation of ordinary containers."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import logging
import os
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.security.account_approval import require_account_approved
from orchestrator.security.crypto import decrypt
from orchestrator.services.generic_harness_runtime import (
    GenericAttemptIdentity,
    GenericBindings,
    GenericBoundFile,
    GenericRuntimeError,
    build_generic_launch,
    pinned_image,
)
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_harness_egress import (
    HarnessEgressConfigurationError,
    validate_harness_egress,
)
from orchestrator.services.manifest_resolution import LiveManifestResolver
from orchestrator.services.manifest_store import ManifestStore, decoded
from shared.manifests.resolution import content_revision

logger = logging.getLogger(__name__)


class ManifestExecutionService:
    def __init__(
        self,
        db,
        *,
        runtime,
        namespace,
        authorize_datasources=None,
        workspace=None,
        srw_image=None,
        cancel_srw=None,
        native_hosting_enabled=False,
        harness_egress="[]",
    ):
        self.db, self.store = db, ManifestStore(db)
        self.runtime, self.namespace = runtime, namespace
        self.authorize_datasources = authorize_datasources
        self.workspace = workspace
        self.srw_image = srw_image
        self.cancel_srw = cancel_srw
        self.native_hosting_enabled = native_hosting_enabled
        self._harness_egress_error = None
        try:
            self._harness_egress = validate_harness_egress(harness_egress)
        except HarnessEgressConfigurationError as exc:
            self._harness_egress, self._harness_egress_error = (), str(exc)

    def _require_harness_egress(self):
        if self._harness_egress_error:
            raise HTTPException(
                503,
                {
                    "code": "HostingConfigurationInvalid",
                    "message": self._harness_egress_error,
                },
            )

    async def admit(self, prepared, resource, user, *, request=None):
        resolved = prepared["resolved"]
        spec = resolved["spec"]
        scope = resolved["metadata"]["scope"]
        authority = ManifestAuthority(self.db, user, request=request)
        await authority.scope(scope, write=True)
        project_id = scope["name"] if scope["kind"] == "Project" else None
        if scope["kind"] == "Catalog":
            raise HTTPException(422, "Jobs execute in Account or Project scope.")
        job_id = uuid4()
        execution = spec["execution"]
        runtime = execution["expert"]["inline"]["runtime"]
        adapter = runtime.get("adapter", "generic")
        if adapter == "generic" and not self.native_hosting_enabled:
            raise HTTPException(
                503,
                {
                    "code": "HostingCapabilityUnavailable",
                    "message": "Generic hosting requires an installation-verified network profile that enforces isolation before container startup.",
                },
            )
        if adapter == "generic":
            self._require_harness_egress()
        if scope["kind"] == "Account" and scope["name"] != str(user["id"]):
            # Publishing into another account never lends the administrator's
            # runtime credentials, grants or model defaults to that account.
            owner = await self.db.get_user(scope["name"])
            if not owner:
                raise HTTPException(403, "Execution account is unavailable.")
            require_account_approved(owner)
            user = owner
        datasource_ids, policy_revisions = [], {}
        for connector in execution["connectors"].values():
            value = connector["inline"]
            if adapter == "srw/v1" and value["driver"] != "srw.datasource/v1":
                raise HTTPException(
                    422,
                    "Connector driver is not installed for the SRW harness adapter; use srw.datasource/v1 with an authorized datasource.",
                )
            if value["driver"] == "srw.datasource/v1":
                if adapter != "srw/v1":
                    raise HTTPException(
                        422,
                        "This datasource driver is implemented by the SRW harness adapter; choose an explicit env/file connector for this image.",
                    )
                try:
                    datasource_ids.append(str(UUID(value["config"]["datasourceId"])))
                except (KeyError, ValueError, TypeError):
                    raise HTTPException(
                        422, "Datasource connectors require config.datasourceId."
                    ) from None
        workspace = execution["workspace"]
        instance_recipe = None
        if adapter == "srw/v1" and workspace and "instanceRef" in workspace:
            from orchestrator.services.retained_vm_workspaces import read_instance

            instance = await read_instance(
                self.db,
                workspace["instanceRef"]["uid"],
                user,
                project_id=project_id,
                request=request,
            )
            instance_recipe = instance["recipe"]
        backend = (
            workspace.get("template", {}).get("inline", {}).get("backend")
            if workspace
            else "none"
        )
        if instance_recipe is not None:
            backend = instance_recipe["backend"]
        if adapter == "srw/v1" and backend == "vm":
            from orchestrator.services.vm_workspace_policy import (
                VmPermissionDependencies,
                check_vm_permission,
            )

            await check_vm_permission(
                user, job_needs_vm=True, dependencies=VmPermissionDependencies(self.db)
            )
        if datasource_ids:
            if not self.authorize_datasources:
                raise HTTPException(503, "Datasource admission is unavailable.")
            datasource_ids, policy_revisions = await self.authorize_datasources(
                user,
                datasource_ids,
                workspace_backend=backend,
                target_project_ids=[project_id] if project_id else [],
                effective_work_owner_id=str(user["id"]),
                trusted_system_inheritance=False,
                legacy_job_id=None,
            )
        config_name, config_override = (
            "worker_base",
            {"workspace": {"backend": backend or "sandbox"}},
        )
        snapshot = {
            key: deepcopy(prepared[key])
            for key in ("document", "resolved", "revision", "dependencies")
        }
        if adapter == "srw/v1":
            from orchestrator.services.manifest_execution_snapshot import (
                prepare_srw_snapshot,
            )
            from orchestrator.services.manifest_runtime_ownership import (
                require_srw_launch_configuration,
            )

            private = require_srw_launch_configuration(
                runtime, trusted_image=self.srw_image
            )
            config_name = private.get("config_name", "worker_base")
            from orchestrator.services.manifest_workspace_selection import (
                srw_workspace_config,
            )

            config_override = {
                "workspace": srw_workspace_config(
                    workspace, instance_recipe=instance_recipe
                )
            }
            if (
                spec["completion"]["mode"] != "Reported"
                or spec["retry"]["maxAttempts"] != 1
            ):
                raise HTTPException(
                    422,
                    "The SRW lifecycle adapter requires Reported completion and one attempt.",
                )
            srw = await prepare_srw_snapshot(
                self.db,
                work_kind="Job",
                work_id=str(job_id),
                owner_id=str(user["id"]),
                project_ids=[project_id] if project_id else [],
                config_name=config_name,
                expert_id=None,
                expert_row={
                    "expert_type": "worker",
                    "config": deepcopy(private.get("config", {})),
                    "prompts": deepcopy(private.get("prompts", {})),
                    "harness_config_layers": deepcopy(private.get("layers", [])),
                    "harness_asset_name": private.get("asset_name"),
                },
                config_override=config_override,
                description=spec.get("task", {}).get("text", ""),
                datasource_ids=datasource_ids,
                policy_revisions=policy_revisions,
            )
            snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
                "config"
            ] = srw["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
                "config"
            ]
            # Keep the authored installation binding, but record the concrete
            # image selected at admission in the immutable execution snapshot.
            snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
                "image"
            ] = self.srw_image
        else:
            bindings = await self.bindings(snapshot, user, materialize=False)
            try:
                build_generic_launch(
                    GenericAttemptIdentity(str(uuid4()), 1),
                    spec,
                    namespace=self.namespace,
                    bindings=bindings,
                    remaining_timeout_seconds=spec.get(
                        "timeoutSeconds",
                        int(os.environ.get("MANIFEST_JOB_TIMEOUT_SECONDS", "3600")),
                    ),
                )
            except (ValueError, GenericRuntimeError) as exc:
                raise HTTPException(422, str(exc)) from None
            if workspace:
                if not self.workspace:
                    raise HTTPException(422, "Native workspace hosting is unavailable.")
                await self.workspace.validate(workspace, user, request=request)
        snapshot["harness_adapter"] = adapter
        snapshot["revision"] = content_revision(snapshot["resolved"]["spec"])
        snapshot["resource"] = resource
        # Domain authority and datasource locks stay in the common insertion
        # funnel. Private image config never passes through jobs.config_override.
        job = await self.db.create_job(
            description=spec.get("task", {}).get("text") or "Manifest assignment",
            origin="user",
            config_name=config_name,
            config_override=config_override,
            context={"manifest_task_data": deepcopy(spec.get("task", {}).get("data"))},
            user_id=str(user["id"]),
            project_id=project_id,
            job_id=job_id,
            requested_workspace_backend=backend or "sandbox",
            execution_lane="pinned",
            datasource_ids=datasource_ids,
            datasource_policy_revisions=policy_revisions,
            datasource_selection_provenance={
                "origin": "manifest",
                "policy_revisions": policy_revisions,
            },
            authority_user_id=str(user["id"]),
            authority_project_ids=[project_id] if project_id else [],
            execution_manifest=snapshot,
        )
        if workspace and adapter == "generic":
            await self.workspace.reserve(str(job["id"]), workspace, user)
        return job["id"]

    async def secret(self, ref, authority, *, materialize):
        await authority.secret(ref["scope"])
        row = await self.db.fetchrow(
            "SELECT ciphertext,keys FROM srw_resource_secrets WHERE scope_kind=$1 AND scope_name=$2 AND name=$3",
            ref["scope"]["kind"],
            ref["scope"]["name"],
            ref["name"],
        )
        if not row or ref["key"] not in row["keys"]:
            raise HTTPException(409, "An execution credential is no longer available.")
        return (
            json.loads(decrypt(row["ciphertext"]))[ref["key"]]
            if materialize
            else "credential-checked"
        )

    async def bindings(self, snapshot, user, *, materialize=True):
        self._require_harness_egress()
        authority = ManifestAuthority(self.db, user)
        resolved = snapshot["resolved"]
        await authority.scope(resolved["metadata"]["scope"], write=True)
        await LiveManifestResolver(self.store, authority).authorize_dependencies(
            snapshot.get("dependencies", [])
        )
        spec = resolved["spec"]
        secret_env, environment, files, descriptor = {}, {}, [], {"connectors": {}}
        for name, value in (
            spec["execution"]["expert"]["inline"]["runtime"].get("env", {}).items()
        ):
            if isinstance(value, dict):
                secret_env[name] = await self.secret(
                    value["secretRef"], authority, materialize=materialize
                )
        for alias, selection in spec["execution"]["connectors"].items():
            connector = selection["inline"]
            driver, config = connector["driver"], connector.get("config", {})
            if driver not in {"srw.env/v1", "srw.files/v1"}:
                raise HTTPException(
                    422, "Connector driver is not installed for generic hosting."
                )
            if "access" in connector:
                raise HTTPException(
                    422,
                    "Env/file delivery cannot enforce ReadOnly/ReadWrite; use credentials scoped by the external resource.",
                )
            values = config.get("env" if driver == "srw.env/v1" else "files", {})
            if not isinstance(values, dict) or set(config) - {
                "env" if driver == "srw.env/v1" else "files"
            }:
                raise HTTPException(422, "Invalid env/file connector configuration.")
            credentials = connector.get("credentials", {})
            for name, value in values.items():
                if (
                    isinstance(value, dict)
                    and set(value) == {"credential"}
                    and value["credential"] in credentials
                ):
                    value = await self.secret(
                        credentials[value["credential"]]["secretRef"],
                        authority,
                        materialize=materialize,
                    )
                if not isinstance(value, str):
                    raise HTTPException(
                        422,
                        "Connector values must be strings or declared credential selections.",
                    )
                if driver == "srw.env/v1":
                    if name in environment:
                        raise HTTPException(
                            422, "Connector environment bindings collide."
                        )
                    environment[name] = value
                else:
                    if not name.startswith("/run/srw/bindings/"):
                        raise HTTPException(
                            422, "Connector files must be under /run/srw/bindings/."
                        )
                    files.append(GenericBoundFile(name, value))
            descriptor["connectors"][alias] = {
                "driver": driver,
                "bindings": list(values),
            }
        return GenericBindings(
            secret_env=secret_env,
            environment=environment,
            files=tuple(files),
            descriptor=descriptor,
            egress=deepcopy(self._harness_egress),
        )

    async def reconcile(self):
        if self.workspace:
            await self.workspace.reconcile_vm_detach()
        if self.cancel_srw:
            from orchestrator.services.execution_deadline import (
                ExecutionDeadline,
                expired_srw_jobs,
            )

            overdue = await expired_srw_jobs(self.db)
            for item in overdue:
                job = await self.db.get_job(str(item["id"]))
                if job:
                    try:
                        await self.cancel_srw(
                            job, expected_execution_deadline=ExecutionDeadline.from_row(item)
                        )
                    except HTTPException:
                        # Control or exact retirement can remain blocked. A
                        # failed cleanup must not starve other overdue owners.
                        logger.warning(
                            "SRW deadline cancellation remains pending for job %s", item["id"]
                        )
        rows = await self.db.fetch("""SELECT s.id FROM srw_execution_specs s JOIN jobs j ON s.work_kind='Job' AND s.work_id=j.id
            WHERE s.harness_adapter='generic' AND (j.status IN ('created','processing') OR EXISTS(
              SELECT 1 FROM srw_execution_attempts a WHERE a.execution_id=s.id AND a.cleaned_at IS NULL))
              OR (s.harness_adapter='generic' AND j.status='pending_review' AND EXISTS(
                SELECT 1 FROM srw_execution_attempts a WHERE a.execution_id=s.id AND a.reported_outcome IS NOT NULL))
              OR (s.harness_adapter='generic' AND j.status IN ('completed','failed','cancelled') AND EXISTS(
                SELECT 1 FROM srw_workspace_instances i WHERE i.execution_id=s.id))
            ORDER BY s.created_at LIMIT 50""")
        for row in rows:
            try:
                await self.reconcile_one(str(row["id"]))
            except Exception:
                # Kubernetes errors and supplied environment must never enter logs.
                logger.error(
                    "Generic execution %s reconciliation needs another observation",
                    row["id"],
                )

    async def reconcile_one(self, execution_id):
        # A session advisory lock spans short external calls. Every restart uses
        # the recorded deterministic attempt identity and observes before acting.
        async with self.db.acquire() as conn:
            lock_key = "srw-generic-execution:" + execution_id
            if not await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key
            ):
                return
            try:
                async with self.db.using_connection(conn):
                    await self._reconcile_one(execution_id)
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key
                )

    async def _reconcile_one(self, execution_id):
        snapshot = decoded(
            await self.db.fetchrow(
                "SELECT * FROM srw_execution_specs WHERE id=$1", UUID(execution_id)
            )
        )
        job = await self.db.get_job(str(snapshot["work_id"]))
        if not job:
            return
        spec = snapshot["resolved"]["spec"]
        latest = await self.db.fetchrow(
            "SELECT * FROM srw_execution_attempts WHERE execution_id=$1 ORDER BY attempt DESC LIMIT 1",
            snapshot["id"],
        )
        elapsed = (datetime.now(timezone.utc) - snapshot["created_at"]).total_seconds()
        remaining = int(
            spec.get(
                "timeoutSeconds",
                int(os.environ.get("MANIFEST_JOB_TIMEOUT_SECONDS", "3600")),
            )
            - elapsed
        )
        active = job["status"] in {"created", "processing"}
        if not latest and not active:
            return
        if not latest:
            async with self.db.transaction_scope():
                await self.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended('srw-generic-capacity',0))"
                )
                if (
                    await self.db.fetchval("""SELECT count(*) FROM srw_execution_specs s JOIN jobs j ON j.id=s.work_id AND s.work_kind='Job'
                    WHERE s.harness_adapter='generic' AND EXISTS(SELECT 1 FROM srw_execution_attempts a
                      WHERE a.execution_id=s.id AND (a.cleaned_at IS NULL OR j.status IN ('created','processing')))""")
                    >= int(os.environ.get("MANIFEST_MAX_CONCURRENT_JOBS", "10"))
                ):
                    return
                identity = GenericAttemptIdentity(execution_id, 1)
                latest = await self.db.fetchrow(
                    "INSERT INTO srw_execution_attempts(execution_id,attempt,pod_name,phase) VALUES($1,1,$2,'Preparing') RETURNING *",
                    snapshot["id"],
                    identity.pod_name,
                )
        identity = GenericAttemptIdentity(execution_id, latest["attempt"])
        if latest["cleaned_at"]:
            if active and remaining <= 0:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    expected_status=job["status"],
                    error_message="Manifest execution exceeded its total deadline.",
                )
                job = await self.db.get_job(str(job["id"]))
            await self._settle_attempt(snapshot, latest, job)
            return
        observed = await self.runtime.observe(
            identity,
            expected_pod_uid=str(latest["pod_uid"]) if latest["pod_uid"] else None,
        )
        if observed.phase == "Replaced":
            if active:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    expected_status=job["status"],
                    error_message="Execution pod identity was replaced; workspace ownership is retained.",
                )
            return
        if observed.pod_uid and not latest["pod_uid"]:
            await self.db.execute(
                "UPDATE srw_execution_attempts SET pod_uid=$3 WHERE execution_id=$1 AND attempt=$2 AND pod_uid IS NULL",
                snapshot["id"],
                identity.attempt,
                observed.pod_uid,
            )
        if observed.pod_absent and latest["pod_uid"]:
            if latest["finished_at"]:
                # Cleanup can finish just before its database acknowledgement.
                # finished_at was written only after terminal evidence.
                observed = replace(
                    observed,
                    pod_uid=latest["pod_uid"],
                    process_exit_code=latest["exit_code"],
                    containers_terminal=True,
                )
                if await self._retire(
                    snapshot,
                    identity,
                    observed,
                    phase=latest["phase"],
                    final=not active
                    or latest["phase"] != "Failed"
                    or identity.attempt >= spec["retry"]["maxAttempts"],
                ):
                    await self._settle_attempt(snapshot, latest, job)
            else:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    error_message="Execution pod disappeared without terminal evidence; workspace ownership is retained.",
                    expected_status=job["status"],
                )
            return
        if not active or remaining <= 0:
            if observed.pod_uid and not observed.containers_terminal:
                await self.runtime.cancel(identity, expected_pod_uid=observed.pod_uid)
                return
            if not observed.pod_absent and not observed.containers_terminal:
                return
            if active:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    error_message="Manifest execution exceeded its total deadline.",
                    expected_status=job["status"],
                )
            await self._retire(
                snapshot,
                identity,
                observed,
                phase="Cancelled" if job["status"] == "cancelled" else "Failed",
                final=True,
            )
            return
        if (
            latest["phase"] == "Preparing"
            and observed.pod_absent
            and not latest["pod_uid"]
        ):
            if not self.native_hosting_enabled:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    expected_status=job["status"],
                    error_message="Native hosting capability is no longer enabled; no new process was launched.",
                )
                return
            user = await self.db.get_user(str(snapshot["owner_id"]))
            if not user:
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    error_message="Execution owner is unavailable.",
                    expected_status=job["status"],
                )
                return
            try:
                require_account_approved(user)
                bindings = await self.bindings(snapshot, user)
                bindings = replace(
                    bindings,
                    descriptor={
                        **(bindings.descriptor or {}),
                        "execution": {
                            "id": execution_id,
                            "workId": str(job["id"]),
                            "attempt": identity.attempt,
                        },
                    },
                )
                if spec["execution"]["workspace"]:
                    bindings = await self.workspace.attach(
                        snapshot, identity, bindings, user
                    )
                    if bindings is None:
                        return
                launch_spec = deepcopy(spec)
                if identity.attempt > 1:
                    prior_image = await self.db.fetchval(
                        "SELECT image_id FROM srw_execution_attempts WHERE execution_id=$1 AND image_id IS NOT NULL ORDER BY attempt LIMIT 1",
                        snapshot["id"],
                    )
                    launch_runtime = launch_spec["execution"]["expert"]["inline"][
                        "runtime"
                    ]
                    launch_runtime["image"] = pinned_image(
                        prior_image, launch_runtime["image"]
                    )
                plan = build_generic_launch(
                    identity,
                    launch_spec,
                    namespace=self.namespace,
                    bindings=bindings,
                    remaining_timeout_seconds=max(1, remaining),
                )
            except (HTTPException, ValueError, GenericRuntimeError):
                await self.db.update_job_status(
                    str(job["id"]),
                    status="failed",
                    error_message="Execution bindings or hosting policy are no longer available.",
                    expected_status=job["status"],
                )
                return
            # Cancellation can commit while workspace preparation is in flight.
            current = await self.db.get_job(str(job["id"]))
            if current["status"] not in {"created", "processing"}:
                return
            observed = await self.runtime.launch(plan)
        if observed.pod_uid:
            await self.db.execute(
                "UPDATE srw_execution_attempts SET pod_uid=$3,image_id=$4,phase='Running' WHERE execution_id=$1 AND attempt=$2 AND phase IN ('Preparing','Running')",
                snapshot["id"],
                identity.attempt,
                observed.pod_uid,
                observed.image_id,
            )
            if job["status"] == "created":
                await self.db.update_job_status(
                    str(job["id"]), status="processing", expected_status="created"
                )
        if not observed.containers_terminal:
            if latest["reported_outcome"]:
                await self.runtime.cancel(identity, expected_pod_uid=observed.pod_uid)
            return
        success = observed.process_exit_code == 0
        mode = spec["completion"]["mode"]
        if mode == "Reported" and success and not latest["reported_outcome"]:
            return
        if latest["reported_outcome"]:
            success = latest["reported_outcome"] == "Succeeded"
        awaiting_review = mode == "Manual" and latest["reported_outcome"] is None
        can_retry = (
            not awaiting_review
            and not success
            and identity.attempt < spec["retry"]["maxAttempts"]
        )
        retired = await self._retire(
            snapshot,
            identity,
            observed,
            phase="Succeeded" if success else "Failed",
            final=not can_retry and not awaiting_review,
        )
        if not retired:
            return
        await self._settle_attempt(
            snapshot,
            {**dict(latest), "phase": "Succeeded" if success else "Failed"},
            job,
        )

    async def _settle_attempt(self, snapshot, attempt, job):
        spec = snapshot["resolved"]["spec"]
        identity = GenericAttemptIdentity(str(snapshot["id"]), attempt["attempt"])
        if job["status"] not in {"created", "processing", "pending_review"}:
            if spec["execution"]["workspace"]:
                await self.workspace.detach(snapshot, identity, final=True)
            return
        mode = spec["completion"]["mode"]
        if mode == "Manual" and attempt.get("reported_outcome") is None:
            if job["status"] != "pending_review":
                await self.db.update_job_status(
                    str(job["id"]),
                    status="pending_review",
                    expected_status=job["status"],
                )
            return
        success = (attempt.get("reported_outcome") or attempt["phase"]) == "Succeeded"
        if (
            not success
            and attempt["phase"] != "Cancelled"
            and attempt["attempt"] < spec["retry"]["maxAttempts"]
        ):
            identity = GenericAttemptIdentity(
                str(snapshot["id"]), attempt["attempt"] + 1
            )
            await self.db.execute(
                "INSERT INTO srw_execution_attempts(execution_id,attempt,pod_name,phase) VALUES($1,$2,$3,'Preparing') ON CONFLICT DO NOTHING",
                snapshot["id"],
                identity.attempt,
                identity.pod_name,
            )
            if job["status"] == "pending_review":
                await self.db.update_job_status(
                    str(job["id"]), status="created", expected_status="pending_review"
                )
            return
        if spec["execution"]["workspace"] and not await self.workspace.detach(
            snapshot, identity, final=True
        ):
            return
        current = await self.db.get_job(str(job["id"]))
        if current["status"] in {"created", "processing", "pending_review"}:
            await self.db.update_job_status(
                str(job["id"]),
                status="completed" if success else "failed",
                expected_status=current["status"],
                error_message=None if success else "Harness process failed.",
            )

    async def _retire(self, snapshot, identity, observed, *, phase, final):
        await self.db.execute(
            "UPDATE srw_execution_attempts SET phase=$3,exit_code=$4,finished_at=COALESCE(finished_at,now()) WHERE execution_id=$1 AND attempt=$2",
            snapshot["id"],
            identity.attempt,
            phase,
            observed.process_exit_code,
        )
        if snapshot["resolved"]["spec"]["execution"]["workspace"]:
            if not await self.workspace.detach(snapshot, identity, final=final):
                return False
        if observed.pod_uid and not await self.runtime.cleanup(
            identity, expected_pod_uid=observed.pod_uid
        ):
            return False
        await self.db.execute(
            "UPDATE srw_execution_attempts SET cleaned_at=now() WHERE execution_id=$1 AND attempt=$2",
            snapshot["id"],
            identity.attempt,
        )
        return True

    async def report_outcome(
        self, resource_id, user, *, attempt, outcome, request=None
    ):
        """Optional external outcome input; the controller still fences processes."""
        if outcome not in {"Succeeded", "Failed"}:
            raise HTTPException(422, "Invalid execution outcome.")
        async with self.db.transaction_scope():
            resource = await self.store.by_id(resource_id)
            if not resource or resource["kind"] != "Job":
                raise HTTPException(404, "Job resource does not exist.")
            await ManifestAuthority(self.db, user, request=request).resource(
                resource, write=True
            )
            snapshot = decoded(
                await self.db.fetchrow(
                    "SELECT * FROM srw_execution_specs WHERE resource_id=$1",
                    resource["id"],
                )
            )
            if not snapshot or snapshot["harness_adapter"] != "generic":
                raise HTTPException(
                    409, "This Job's outcome belongs to its SRW lifecycle adapter."
                )
            if snapshot["resolved"]["spec"]["completion"]["mode"] not in {
                "Reported",
                "Manual",
            }:
                raise HTTPException(
                    409, "ProcessExit Jobs complete through observed process exit."
                )
            await self.db.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                "srw-generic-execution:" + str(snapshot["id"]),
            )
            latest = await self.db.fetchrow(
                "SELECT * FROM srw_execution_attempts WHERE execution_id=$1 ORDER BY attempt DESC LIMIT 1 FOR UPDATE",
                snapshot["id"],
            )
            job = await self.db.get_job(str(snapshot["work_id"]))
            if not latest or latest["attempt"] != attempt:
                raise HTTPException(
                    409,
                    "Execution attempt changed; read current status before reporting.",
                )
            if latest["reported_outcome"] == outcome:
                return {"accepted": True, "attempt": attempt, "outcome": outcome}
            if latest["reported_outcome"] or job["status"] not in {
                "created",
                "processing",
                "pending_review",
            }:
                raise HTTPException(409, "The execution outcome is already settled.")
            await self.db.execute(
                "UPDATE srw_execution_attempts SET reported_outcome=$3 WHERE execution_id=$1 AND attempt=$2",
                snapshot["id"],
                attempt,
                outcome,
            )
        return {"accepted": True, "attempt": attempt, "outcome": outcome}

    async def cancel(self, work_id):
        snapshot = await self.store.execution("Job", work_id)
        if not snapshot or snapshot["harness_adapter"] != "generic":
            return False
        job = await self.db.get_job(work_id)
        if job["status"] not in {"completed", "cancelled"}:
            await self.db.update_job_status(
                work_id, status="cancelled", expected_status=job["status"]
            )
        await self.reconcile_one(str(snapshot["id"]))
        return True
