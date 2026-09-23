"""Disposable acceptance-only object retention; never manufactures stop proof."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import re
import time
from uuid import UUID

FINALIZER = "srw.io/vm-recovery-gate-stop-evidence"
MARKER = "srw.io/vm-recovery-gate-stop-operation"
FENCE = "srw.io/vm-recovery-gate-stop-settled"
CONTEXT_KEY = "vm_workspace_recovery_gate_stop_control"


class GateStopError(RuntimeError):
    """Bounded fixture diagnostic, without Kubernetes response bodies."""


def validate_document(doc):
    try:
        if set(doc) != {
            "version",
            "run_id",
            "job_id",
            "user_id",
            "operation_id",
            "generation",
            "namespace",
            "vm_name",
            "vm_uid",
            "vmi_name",
            "vmi_uid",
            "pod_name",
            "pod_uid",
            "pvc_uid",
            "original_strategy",
            "stage",
        }:
            raise ValueError
        if type(doc["version"]) is not int or doc["version"] != 1:
            raise ValueError
        for key in (
            "job_id",
            "user_id",
            "operation_id",
            "generation",
            "vm_uid",
            "vmi_uid",
            "pod_uid",
            "pvc_uid",
        ):
            if str(UUID(doc[key])) != doc[key]:
                raise ValueError
        for key in ("namespace", "vm_name", "vmi_name", "pod_name"):
            if not isinstance(doc[key], str) or not re.fullmatch(
                r"[a-z0-9][a-z0-9.-]{0,251}", doc[key]
            ):
                raise ValueError
        if not isinstance(doc["run_id"], str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,100}", doc["run_id"]
        ):
            raise ValueError
        if (
            doc["vm_name"] != "agent-vm-" + doc["job_id"]
            or doc["vmi_name"] != doc["vm_name"]
        ):
            raise ValueError
        if doc["original_strategy"] != "RerunOnFailure" or doc["stage"] not in (
            "planned",
            "held",
            "released",
            "restored",
            "aborted",
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise GateStopError("invalid_stop_control") from None


class GateStopControl:
    def __init__(self, kube, store, *, timeout=60, interval=0.25):
        self.kube, self.store = kube, store
        self.timeout, self.interval = timeout, interval

    def _exact(self, doc, kind, obj, *, deleting=False):
        validate_document(doc)
        if obj is None:
            return None
        try:
            meta = obj["metadata"]
            if (
                meta["uid"] != doc[kind + "_uid"]
                or meta["name"] != doc[kind + "_name"]
                or meta["namespace"] != doc["namespace"]
                or not isinstance(meta["resourceVersion"], str)
                or not meta["resourceVersion"]
                or (not deleting and meta.get("deletionTimestamp"))
            ):
                raise ValueError
            finalizers = meta.get("finalizers", [])
            annotations = meta.get("annotations", {})
            if (
                not isinstance(finalizers, list)
                or any(not isinstance(x, str) for x in finalizers)
                or len(set(finalizers)) != len(finalizers)
                or not isinstance(annotations, dict)
                or annotations.get(MARKER) not in (None, doc["operation_id"])
                or (
                    FINALIZER in finalizers
                    and annotations.get(MARKER) != doc["operation_id"]
                )
            ):
                raise ValueError
            if kind == "vm":
                if (
                    meta.get("labels", {}).get("srw.io/owner-id") != doc["job_id"]
                    or meta.get("labels", {}).get("srw.io/owner-kind") != "job"
                    or annotations.get("srw.io/provision-generation")
                    != doc["generation"]
                ):
                    raise ValueError
            else:
                if kind == "vmi" and not deleting:
                    migration = obj.get("status", {}).get("migrationState")
                    if migration is not None and (
                        not isinstance(migration, dict) or migration
                    ):
                        raise ValueError
                owner_kind, owner_key = (
                    ("VirtualMachine", "vm")
                    if kind == "vmi"
                    else ("VirtualMachineInstance", "vmi")
                )
                owners = [
                    x
                    for x in meta.get("ownerReferences", [])
                    if x.get("controller") is True
                ]
                if len(owners) != 1 or any(
                    owners[0].get(k) != v
                    for k, v in {
                        "apiVersion": "kubevirt.io/v1",
                        "kind": owner_kind,
                        "uid": doc[owner_key + "_uid"],
                        "name": doc[owner_key + "_name"],
                    }.items()
                ):
                    raise ValueError
            return obj
        except (KeyError, TypeError, ValueError, AttributeError):
            raise GateStopError("stop_object_identity_changed") from None

    async def _mutate(
        self, doc, kind, build, *, deleting=False, absent=False, check=None
    ):
        for _ in range(4):
            obj = self._exact(
                doc, kind, await self.kube.read(doc, kind), deleting=deleting
            )
            if obj is None:
                if absent:
                    return None
                raise GateStopError("stop_object_missing")
            body = build(obj)
            if body is None:
                return obj
            body.setdefault("metadata", {}).update(
                uid=obj["metadata"]["uid"],
                resourceVersion=obj["metadata"]["resourceVersion"],
            )
            if check is not None:
                await check()
            try:
                await self.kube.patch(doc, kind, body)
            except GateStopError:
                # A lost response is not failure proof. Read back before retrying.
                pass
        obj = self._exact(doc, kind, await self.kube.read(doc, kind), deleting=deleting)
        if obj is None and absent:
            return None
        if obj is not None and build(obj) is None:
            return obj
        raise GateStopError("stop_patch_unsettled")

    async def _hold(self, doc, kind):
        def build(obj):
            meta = obj["metadata"]
            if meta.get("annotations", {}).get(FENCE):
                raise GateStopError("stop_retention_already_settled")
            if FINALIZER in meta.get("finalizers", []):
                return None
            return {
                "metadata": {
                    "finalizers": [*meta.get("finalizers", []), FINALIZER],
                    "annotations": {MARKER: doc["operation_id"]},
                }
            }

        await self._mutate(doc, kind, build)

    async def _strategy(
        self, doc, strategy, *, absent=False, check=None, fence=False, acknowledge=False
    ):
        def build(obj):
            if obj.get("status", {}).get("stateChangeRequests"):
                raise GateStopError("stop_vm_request_pending")
            current = obj.get("spec", {}).get("runStrategy")
            allowed = {
                "Manual": ("RerunOnFailure", "Manual"),
                "RerunOnFailure": ("Halted", "RerunOnFailure"),
                "Halted": ("RerunOnFailure", "Manual", "Halted"),
            }[strategy]
            if current not in allowed:
                raise GateStopError("stop_strategy_changed")
            settled = obj["metadata"].get("annotations", {}).get(FENCE)
            if strategy != "Halted" and settled:
                raise GateStopError("stop_control_already_aborted")
            if current == strategy and (not fence or settled == doc["operation_id"]):
                return None
            body = {"spec": {"runStrategy": strategy}}
            if fence:
                # A timed-out HTTP request may still exist server-side. Change
                # RV even when already Halted so its old restore CAS must lose.
                body["metadata"] = {"annotations": {FENCE: doc["operation_id"]}}
            return body

        result = await self._mutate(doc, "vm", build, absent=absent, check=check)
        if result is None:
            return
        stop = time.monotonic() + self.timeout
        while time.monotonic() < stop:
            obj = self._exact(doc, "vm", await self.kube.read(doc, "vm"))
            if obj is None and absent:
                return
            if obj is None or obj.get("spec", {}).get("runStrategy") != strategy:
                raise GateStopError("stop_strategy_changed")
            if obj.get("status", {}).get("stateChangeRequests"):
                raise GateStopError("stop_vm_request_pending")
            if check is not None:
                await check()
            if strategy != "Manual" and not acknowledge:
                return
            generation = obj["metadata"].get("generation")
            if (
                type(generation) is int
                and obj.get("status", {}).get("desiredGeneration") == generation
            ):
                return
            await asyncio.sleep(self.interval)
        raise GateStopError("stop_strategy_unacknowledged")

    async def _remove(self, doc, kind):
        def build(obj):
            meta = obj["metadata"]
            if (
                FINALIZER not in meta.get("finalizers", [])
                and meta.get("annotations", {}).get(FENCE) == doc["operation_id"]
            ):
                return None
            return {
                "metadata": {
                    "finalizers": [
                        x for x in meta.get("finalizers", []) if x != FINALIZER
                    ],
                    "annotations": {MARKER: None, FENCE: doc["operation_id"]},
                }
            }

        await self._mutate(doc, kind, build, deleting=True, absent=True)
        stop = time.monotonic() + self.timeout
        while time.monotonic() < stop:
            if (
                self._exact(doc, kind, await self.kube.read(doc, kind), deleting=True)
                is None
            ):
                return
            await asyncio.sleep(self.interval)
        raise GateStopError("stop_old_object_still_present")

    async def _stage(self, doc, stage):
        changed = {**doc, "stage": stage}
        await self.store.save(changed)
        doc.update(changed)

    async def prepare(self, doc):
        validate_document(doc)
        if doc["stage"] != "planned":
            raise GateStopError("stop_stage_changed")
        for kind in ("vm", "vmi", "pod"):
            obj = self._exact(doc, kind, await self.kube.read(doc, kind))
            if obj is None:
                raise GateStopError("stop_object_missing")
            if (
                kind == "vm"
                and obj.get("spec", {}).get("runStrategy") != doc["original_strategy"]
            ):
                raise GateStopError("stop_strategy_changed")
        await self.store.save(doc)  # committed write-ahead before any object patch
        async with self.store.authorized(doc):
            await self.kube.assert_quiet(doc)
            await self._hold(doc, "vmi")
            await self._hold(doc, "pod")
            await self._strategy(doc, "Manual")
            await self.kube.assert_quiet(doc)
            for kind in ("vmi", "pod"):
                obj = self._exact(doc, kind, await self.kube.read(doc, kind))
                if obj is None or FINALIZER not in obj["metadata"].get(
                    "finalizers", []
                ):
                    raise GateStopError("stop_retention_missing")
        await self._stage(doc, "held")

    async def release(self, doc):
        if doc["stage"] != "held":
            raise GateStopError("stop_stage_changed")
        async with self.store.authorized(doc, receipt=True):
            await self._strategy(doc, "Halted", acknowledge=True)
            await self._remove(doc, "pod")
            await self._remove(doc, "vmi")
        await self._stage(doc, "released")

    async def restore(self, doc):
        if doc["stage"] != "released":
            raise GateStopError("stop_stage_changed")
        async with self.store.authorized(doc, receipt=True) as check:
            for kind in ("pod", "vmi"):
                if await self.kube.read(doc, kind) is not None:
                    raise GateStopError("stop_old_object_still_present")
            await self._strategy(doc, doc["original_strategy"], check=check)
        await self._stage(doc, "restored")

    async def abort(self, doc):
        validate_document(doc)
        await self.store.cancel(doc)
        vm = self._exact(doc, "vm", await self.kube.read(doc, "vm"), deleting=True)
        if vm is not None and not vm["metadata"].get("deletionTimestamp"):
            await self._strategy(doc, "Halted", absent=True, fence=True)
        await self._remove(doc, "pod")
        await self._remove(doc, "vmi")
        await self._stage(doc, "aborted")


async def joined_call(function, *args, **kwargs):
    """Do not let cancellation leave a Kubernetes mutation running in a thread."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()  # consume a failed transport before propagating cancellation
        raise


class KubernetesStopObjects:
    """Explicit merge PATCH replaces just our freshly read finalizer array."""

    _response_types = {200: "object", 201: "object", 202: "object", 204: None}

    def __init__(self, core):
        self.api = core.api_client

    async def _call(self, doc, kind, method, body=None):
        validate_document(doc)
        prefix = "/api/v1" if kind == "pod" else "/apis/kubevirt.io/v1"
        plural = {
            "pod": "pods",
            "vm": "virtualmachines",
            "vmi": "virtualmachineinstances",
        }[kind]
        path = f"{prefix}/namespaces/{doc['namespace']}/{plural}/{doc[kind + '_name']}"
        try:
            return await joined_call(
                self.api.call_api,
                path,
                method,
                body=body,
                response_types_map=self._response_types,
                auth_settings=["BearerToken"],
                header_params={
                    "Accept": "application/json",
                    "Content-Type": "application/merge-patch+json",
                },
                _return_http_data_only=True,
                _request_timeout=(3, 5),
            )
        except Exception as exc:
            if method == "GET" and getattr(exc, "status", None) == 404:
                return None
            raise GateStopError("stop_transport_uncertain") from None

    async def fixture_objects(self, namespace, job_id):
        if str(UUID(job_id)) != job_id or not re.fullmatch(
            r"[a-z0-9][a-z0-9.-]{0,251}", namespace
        ):
            raise GateStopError("purge_fixture_changed")
        result = {}
        for kind, prefix, plural in (
            ("vm", "/apis/kubevirt.io/v1", "virtualmachines"),
            ("vmi", "/apis/kubevirt.io/v1", "virtualmachineinstances"),
            ("dv", "/apis/cdi.kubevirt.io/v1beta1", "datavolumes"),
            ("pvc", "/api/v1", "persistentvolumeclaims"),
        ):
            name = "agent-vm-" + job_id + ("-rootdisk" if kind in ("dv", "pvc") else "")
            try:
                result[kind] = await joined_call(
                    self.api.call_api,
                    f"{prefix}/namespaces/{namespace}/{plural}/{name}",
                    "GET",
                    response_types_map=self._response_types,
                    auth_settings=["BearerToken"],
                    header_params={"Accept": "application/json"},
                    _return_http_data_only=True,
                    _request_timeout=(3, 5),
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    result[kind] = None
                else:
                    raise GateStopError("purge_observation_unknown") from None
        return result

    async def assert_quiet(self, doc):
        validate_document(doc)
        for plural, prefix, limit in (
            ("pods", "/api/v1", 3),
            ("virtualmachineinstancemigrations", "/apis/kubevirt.io/v1", 100),
        ):
            query = [("limit", limit)]
            if plural == "pods":
                query.append(("labelSelector", "vm.kubevirt.io/name=" + doc["vm_name"]))
            try:
                reply = await joined_call(
                    self.api.call_api,
                    f"{prefix}/namespaces/{doc['namespace']}/{plural}",
                    "GET",
                    query_params=query,
                    response_types_map=self._response_types,
                    auth_settings=["BearerToken"],
                    header_params={"Accept": "application/json"},
                    _return_http_data_only=True,
                    _request_timeout=(3, 5),
                )
                items = reply["items"]
                if (
                    not isinstance(items, list)
                    or len(items) > limit
                    or reply.get("metadata", {}).get("continue")
                ):
                    raise ValueError
                if plural == "pods":
                    if len(items) != 1 or items[0]["metadata"]["uid"] != doc["pod_uid"]:
                        raise ValueError
                else:
                    for item in items:
                        if item["metadata"]["namespace"] != doc["namespace"]:
                            raise ValueError
                        name = item["spec"]["vmiName"]
                        if not isinstance(name, str) or not name:
                            raise ValueError
                        if name == doc["vmi_name"] and item.get("status", {}).get(
                            "phase"
                        ) not in ("Succeeded", "Failed"):
                            raise ValueError
            except Exception:
                raise GateStopError("stop_runtime_ambiguous") from None

    async def read(self, doc, kind):
        return await self._call(doc, kind, "GET")

    async def patch(self, doc, kind, body):
        return await self._call(doc, kind, "PATCH", body)


class GateStopStore:
    """Fixture metadata only. The production store exclusively admits receipts."""

    def __init__(self, db, run_id):
        self.db, self.run_id = db, run_id

    @staticmethod
    def _context(value):
        import json

        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            raise GateStopError("stop_fixture_changed")
        return value

    def _job(self, doc, row):
        validate_document(doc)
        if (
            row is None
            or doc["run_id"] != self.run_id
            or str(row["id"]) != doc["job_id"]
            or str(row["user_id"]) != doc["user_id"]
            or row["execution_lane"] != "stateless"
            or row["description"]
            != f"[vm-recovery-gate:{self.run_id}] retained disk fixture"
        ):
            raise GateStopError("stop_fixture_changed")
        context = self._context(row["context"])
        if context.get("vm_workspace_recovery_acceptance_gate") != self.run_id:
            raise GateStopError("stop_fixture_changed")
        return context

    @staticmethod
    def _recovery(doc, row):
        if row is None or any(
            str(row[key]) != doc[expected]
            for key, expected in (
                ("id", "operation_id"),
                ("owner_id", "job_id"),
                ("namespace", "namespace"),
                ("provision_generation", "generation"),
                ("vm_uid", "vm_uid"),
                ("prior_vmi_uid", "vmi_uid"),
                ("prior_launcher_uid", "pod_uid"),
                ("root_pvc_uid", "pvc_uid"),
            )
        ):
            raise GateStopError("stop_recovery_changed")
        if row["owner_kind"] != "job":
            raise GateStopError("stop_recovery_changed")

    async def save(self, doc):
        import json

        validate_document(doc)
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", UUID(doc["job_id"])
            )
            context = self._job(doc, row)
            recovery = await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1",
                UUID(doc["operation_id"]),
            )
            self._recovery(doc, recovery)
            previous = context.get(CONTEXT_KEY)
            if previous is not None:
                validate_document(previous)
                if {k: v for k, v in previous.items() if k != "stage"} != {
                    k: v for k, v in doc.items() if k != "stage"
                }:
                    raise GateStopError("stop_control_changed")
                transitions = {
                    "planned": {"planned", "held", "aborted"},
                    "held": {"held", "released", "aborted"},
                    "released": {"released", "restored", "aborted"},
                    "restored": {"restored", "aborted"},
                    "aborted": {"aborted"},
                }
                if doc["stage"] not in transitions[previous["stage"]]:
                    raise GateStopError("stop_stage_changed")
            elif doc["stage"] != "planned":
                raise GateStopError("stop_control_missing")
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,$2::text[],$3::jsonb) WHERE id=$1",
                UUID(doc["job_id"]),
                [CONTEXT_KEY],
                json.dumps(doc),
            )

    async def load(self):
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM jobs WHERE context->>'vm_workspace_recovery_acceptance_gate'=$1 AND context ? $2",
                self.run_id,
                CONTEXT_KEY,
            )
        result = []
        for row in rows:
            doc = self._context(row["context"])[CONTEXT_KEY]
            self._job(doc, row)
            result.append(doc)
        return result

    @asynccontextmanager
    async def authorized(self, doc, *, receipt=False):
        validate_document(doc)
        async with self.db.acquire() as conn, conn.transaction():
            # Match the existing recovery writer order: Job, then recovery;
            # never acquire a queue/owner lock after these rows.
            row = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", UUID(doc["job_id"])
            )
            context = self._job(doc, row)
            if context.get(CONTEXT_KEY) != doc:
                raise GateStopError("stop_control_changed")
            recovery = await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1 FOR UPDATE",
                UUID(doc["operation_id"]),
            )
            self._recovery(doc, recovery)

            async def check():
                active = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_workspace_recoveries r JOIN vm_workspace_recovery_jobs p ON p.recovery_id=r.id JOIN jobs j ON j.id=p.job_id WHERE r.id=$1 AND p.job_id=$2 AND r.resolved_at IS NULL AND r.phase NOT IN ('paused_attention','cancelled','recovered','superseded') AND p.resolved_at IS NULL AND p.participation='held' AND j.status NOT IN ('cancelled','completed','failed') AND r.deadline_at>clock_timestamp()+interval '10 seconds')",
                    UUID(doc["operation_id"]),
                    UUID(doc["job_id"]),
                )
                if not active:
                    raise GateStopError("stop_authority_expired")

            await check()  # fresh DB clock after every lock wait
            if receipt:
                found = await conn.fetchrow(
                    "SELECT * FROM vm_workspace_recovery_stop_receipts WHERE recovery_id=$1 AND vm_uid=$2 AND vmi_uid=$3 AND launcher_uid=$4 AND root_pvc_uid=$5 ORDER BY accepted_at DESC LIMIT 1",
                    *[
                        UUID(doc[k])
                        for k in (
                            "operation_id",
                            "vm_uid",
                            "vmi_uid",
                            "pod_uid",
                            "pvc_uid",
                        )
                    ],
                )
                if (
                    not found
                    or not found["controller_identity"]
                    or not found["container_id"]
                    or found["accepted_claim_token"] <= 0
                    or re.fullmatch(r"sha256:[a-f0-9]{64}", found["evidence_digest"])
                    is None
                ):
                    raise GateStopError("stop_receipt_missing")
            yield check

    async def cancel(self, doc):
        validate_document(doc)
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", UUID(doc["job_id"])
            )
            context = self._job(doc, row)
            previous = context.get(CONTEXT_KEY)
            validate_document(previous)
            if previous is None or {
                k: v for k, v in previous.items() if k != "stage"
            } != {k: v for k, v in doc.items() if k != "stage"}:
                raise GateStopError("stop_control_changed")
            recovery = await conn.fetchrow(
                "SELECT * FROM vm_workspace_recoveries WHERE id=$1 FOR UPDATE",
                UUID(doc["operation_id"]),
            )
            self._recovery(doc, recovery)
            await conn.execute(
                "UPDATE vm_workspace_recovery_jobs SET participation='cancelled',resolved_at=COALESCE(resolved_at,clock_timestamp()) WHERE recovery_id=$1 AND job_id=$2 AND resolved_at IS NULL",
                UUID(doc["operation_id"]),
                UUID(doc["job_id"]),
            )
            await conn.execute(
                "UPDATE vm_workspace_recoveries SET phase='cancelled',resolved_at=COALESCE(resolved_at,clock_timestamp()) WHERE id=$1",
                UUID(doc["operation_id"]),
            )
            await conn.execute(
                "UPDATE vm_workspace_recovery_retention_pins SET released_at=COALESCE(released_at,clock_timestamp()) WHERE recovery_id=$1",
                UUID(doc["operation_id"]),
            )


class GateFixturePurge:
    """Normal guarded purge plus durable exact-UID absence verification."""

    KEY = "vm_workspace_recovery_gate_purge"
    KINDS = ("vm", "vmi", "dv", "pvc")

    def __init__(
        self,
        db,
        kube,
        delete,
        run_id,
        namespace,
        *,
        anchor=None,
        timeout=180,
        interval=1,
    ):
        self.anchor = anchor
        self.db, self.kube, self.delete = db, kube, delete
        self.run_id, self.namespace = run_id, namespace
        self.timeout, self.interval = timeout, interval

    async def _scope(self, conn, job_id):
        row = await conn.fetchrow(
            "SELECT j.*,u.display_name AS gate_principal,u.is_admin AS gate_admin "
            "FROM jobs j JOIN users u ON u.id=j.user_id WHERE j.id=$1 FOR UPDATE OF j",
            job_id,
        )
        if (
            row is None
            or row["execution_lane"] != "stateless"
            or row["description"]
            != f"[vm-recovery-gate:{self.run_id}] retained disk fixture"
            or row["gate_principal"] != f"VM recovery gate {self.run_id}"
            or row["gate_admin"] is not False
        ):
            raise GateStopError("purge_fixture_changed")
        context = GateStopStore._context(row["context"])
        if context.get("vm_workspace_recovery_acceptance_gate") != self.run_id:
            raise GateStopError("purge_fixture_changed")
        return row, context

    def _identities(self, job_id, objects):
        try:
            if set(objects) != set(self.KINDS):
                raise ValueError
            result = {}
            for kind, obj in objects.items():
                if obj is None:
                    result[kind] = None
                    continue
                meta = obj["metadata"]
                name = (
                    "agent-vm-"
                    + str(job_id)
                    + ("-rootdisk" if kind in ("dv", "pvc") else "")
                )
                uid = meta["uid"]
                if (
                    str(UUID(uid)) != uid
                    or meta["name"] != name
                    or meta["namespace"] != self.namespace
                    or meta.get("labels", {}).get("srw.io/owner-kind") != "job"
                    or meta.get("labels", {}).get("srw.io/owner-id") != str(job_id)
                ):
                    raise ValueError
                result[kind] = uid
            for child, parent, group, kind in (
                ("vmi", "vm", "kubevirt.io/v1", "VirtualMachine"),
                ("pvc", "dv", "cdi.kubevirt.io/v1beta1", "DataVolume"),
            ):
                if objects[child] is None:
                    continue
                refs = [
                    ref
                    for ref in objects[child]["metadata"].get("ownerReferences", [])
                    if ref.get("controller") is True
                ]
                parent_name = (
                    "agent-vm-" + str(job_id) + ("-rootdisk" if parent == "dv" else "")
                )
                if (
                    len(refs) != 1
                    or refs[0].get("apiVersion") != group
                    or refs[0].get("kind") != kind
                    or refs[0].get("name") != parent_name
                    or (
                        result[parent] is not None
                        and refs[0].get("uid") != result[parent]
                    )
                ):
                    raise ValueError
            return result
        except (ValueError, TypeError, KeyError, AttributeError):
            raise GateStopError("purge_object_identity_changed") from None

    @staticmethod
    def _matches(actual, expected):
        if any(
            value is not None and value != expected.get(kind)
            for kind, value in actual.items()
        ):
            raise GateStopError("purge_object_recreated")

    async def _cleanup_completed(self, job_id, snapshot):
        pvc_uid = snapshot["uids"]["pvc"]
        if pvc_uid is None:
            return True
        dv_uid = snapshot["uids"]["dv"]
        if dv_uid is None:
            raise GateStopError("purge_cleanup_identity_unproven")
        identity = {
            "source": "controller_rootdisk_delete",
            "owner_kind": "job",
            "owner_id": str(job_id),
            "pvc_uid": pvc_uid,
            "dv_uid": dv_uid,
            "provision_generation": snapshot["generation"],
        }
        intent_digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode("utf-8")
            ).hexdigest()
        )
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT completed_at,outcome,intent_digest "
                "FROM vm_workspace_cleanup_admissions "
                "WHERE owner_kind='job' AND owner_id=$1 AND pvc_uid=$2 "
                "AND source='controller_rootdisk_delete'",
                job_id,
                UUID(pvc_uid),
            )
        if len(rows) != 1:
            raise GateStopError("purge_cleanup_receipt_missing")
        if rows[0]["intent_digest"] != intent_digest:
            raise GateStopError("purge_cleanup_receipt_changed")
        if rows[0]["completed_at"] is None:
            return False
        if rows[0]["outcome"] != "deleted":
            raise GateStopError("purge_cleanup_receipt_changed")
        return True

    async def _await_cleanup_completed(self, job_id, snapshot, stop_at):
        while True:
            current = self._identities(
                job_id, await self.kube.fixture_objects(self.namespace, str(job_id))
            )
            self._matches(current, snapshot["uids"])
            if any(current.values()):
                raise GateStopError("purge_resources_still_present")
            if await self._cleanup_completed(job_id, snapshot):
                return
            if time.monotonic() >= stop_at:
                raise GateStopError("purge_cleanup_unresolved")
            await asyncio.sleep(self.interval)

    async def run(self, job_id):
        job_id = UUID(str(job_id))
        objects = await self.kube.fixture_objects(self.namespace, str(job_id))
        identities = self._identities(job_id, objects)
        anchor = self.anchor
        if anchor is not None:
            try:
                if set(anchor) != {"generation", "vm", "pvc"}:
                    raise ValueError
                if str(UUID(anchor["generation"])) != anchor["generation"]:
                    raise ValueError
                for kind in ("vm", "pvc"):
                    value = anchor[kind]
                    if value is not None and str(UUID(value)) != value:
                        raise ValueError
                    if identities[kind] is not None and identities[kind] != value:
                        raise ValueError
            except (ValueError, TypeError, KeyError, AttributeError):
                raise GateStopError("purge_capture_changed") from None
        elif any(identities.values()):
            raise GateStopError("purge_identity_unproven")
        async with self.db.acquire() as conn, conn.transaction():
            row, context = await self._scope(conn, job_id)
            if anchor is None:
                # No identity is needed to verify all four resources absent;
                # this branch never sends a delete or creates purge authority.
                return
            snapshot = {
                "version": 1,
                "generation": anchor["generation"],
                "run_id": self.run_id,
                "job_id": str(job_id),
                "user_id": str(row["user_id"]),
                "namespace": self.namespace,
                "uids": {**identities, "vm": anchor["vm"], "pvc": anchor["pvc"]},
            }
            previous = context.get(self.KEY)
            if previous is not None:
                if (
                    not isinstance(previous, dict)
                    or set(previous) != set(snapshot)
                    or any(
                        previous.get(k) != v for k, v in snapshot.items() if k != "uids"
                    )
                    or not isinstance(previous.get("uids"), dict)
                    or set(previous["uids"]) != set(self.KINDS)
                ):
                    raise GateStopError("purge_snapshot_changed")
                for value in previous["uids"].values():
                    if value is not None:
                        try:
                            if str(UUID(value)) != value:
                                raise ValueError
                        except (ValueError, TypeError, AttributeError):
                            raise GateStopError("purge_snapshot_changed") from None
                snapshot = previous
                self._matches(identities, snapshot["uids"])
                self._matches(
                    {"vm": anchor["vm"], "pvc": anchor["pvc"]}, snapshot["uids"]
                )
            else:
                # Bind VM/PVC to the persisted fixture identity when one exists.
                stop = context.get(CONTEXT_KEY)
                if stop is not None:
                    validate_document(stop)
                    if (
                        stop["run_id"] != self.run_id
                        or stop["job_id"] != str(job_id)
                        or stop["user_id"] != str(row["user_id"])
                        or stop["namespace"] != self.namespace
                        or stop["generation"] != anchor["generation"]
                    ):
                        raise GateStopError("purge_snapshot_changed")
                    for key, field in (("vm", "vm_uid"), ("pvc", "pvc_uid")):
                        # The authenticated capture authorizes the later
                        # delete even when an object is already absent. Bind
                        # it directly, rather than validating only inventory.
                        if anchor[key] != stop[field]:
                            raise GateStopError("purge_object_recreated")
                await conn.execute(
                    "UPDATE jobs SET context=jsonb_set(context,$2::text[],$3::jsonb) WHERE id=$1",
                    job_id,
                    [self.KEY],
                    json.dumps(snapshot),
                )
        if not any(identities.values()):
            await self._await_cleanup_completed(
                job_id, snapshot, time.monotonic() + self.timeout
            )
            return
        # The normal provisioner retains its process-zero and cleanup-admission
        # checks. A request response is not proof of physical resource absence.
        try:
            accepted = await self.delete(str(job_id), purge_disk=True)
        except Exception:
            raise GateStopError("purge_request_failed") from None
        if accepted is not True:
            raise GateStopError("purge_request_refused")
        stop_at = time.monotonic() + self.timeout
        while time.monotonic() < stop_at:
            current = self._identities(
                job_id, await self.kube.fixture_objects(self.namespace, str(job_id))
            )
            self._matches(current, snapshot["uids"])
            if not any(current.values()):
                await self._await_cleanup_completed(job_id, snapshot, stop_at)
                return
            await asyncio.sleep(self.interval)
        raise GateStopError("purge_resources_still_present")
