"""Initialize one disposable, unleased A1 Job through production writers.

This is a gated test fixture. It creates no worker claim or Ready assertion;
the controller and the separate acceptance command establish those later.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import yaml


_OWNED = re.compile(r"srw-a1-[a-z0-9][a-z0-9-]{2,50}")


class FixtureRefusal(RuntimeError):
    """The disposable source, owner or model is not exact."""


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, dict) else {}


def _expert_config(model_id: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    groups: set[str] = set()
    for relative in ("config/expert_base.yaml", "config/overlays/worker.yaml"):
        document = yaml.safe_load((root / relative).read_text(encoding="utf-8"))
        groups.update((document.get("tools") or {}).keys())
    tools = {name: [] for name in groups}
    tools.update(
        workspace=["read_file"],
        shell=["run_command"],
        core=["next_phase_todos", "todo_complete", "job_complete"],
    )
    return {
        "llm": {"model": model_id, "temperature": 0.17},
        "auxiliary": {"model": model_id, "enabled": False},
        "memory": {"enabled": False},
        "tools": tools,
        "limits": {"max_tool_calls_per_job": 50},
        "autonomy": "full",
        "instruction_files": [],
        "verification": {"enabled": False},
        "curator": {"enabled": False},
        "scholar": {"enabled": False},
        "delegation": {"enabled": False},
    }


async def seed_model(
    db: Any, *, run_id: str, namespace: str, model_id: str,
    inference_key: str,
) -> str:
    """Register/replay only the run-owned authenticated provider transport."""
    if (
        not _OWNED.fullmatch(run_id)
        or len(f"srw-a1-provider-{run_id}") > 63
        or namespace != run_id
        or not re.fullmatch(r"e2e-vm-[a-z0-9-]{3,55}", model_id)
        or not isinstance(inference_key, str)
        or not 16 <= len(inference_key) <= 256
        or "\n" in inference_key
        or os.environ.get("VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED") != "true"
    ):
        raise FixtureRefusal("A1 deterministic provider seed is not exact")
    job_id = uuid5(NAMESPACE_URL, f"srw-a1-job:{run_id}")
    if await db.fetchval("SELECT EXISTS(SELECT 1 FROM jobs WHERE id<>$1)", job_id):
        raise FixtureRefusal("A1 fixture database contains another Job")
    label = f"srw-a1-provider-{run_id}"
    base_url = f"http://srw-a1-provider.{namespace}.svc.cluster.local:8000/v1"
    existing_model = await db.resolve_catalog_model(model_id, capability="chat")
    endpoint_row = await db.fetchrow(
        "SELECT id FROM llm_endpoints WHERE label=$1", label,
    )
    if endpoint_row is None:
        if existing_model or await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM models WHERE model_id=$1)", model_id,
        ):
            raise FixtureRefusal("A1 model has no matching owned endpoint")
        endpoint = await db.create_system_llm_endpoint(
            label=label, base_url=base_url, api_key=inference_key,
            key_prefix=None, source="ui",
        )
    else:
        endpoint = await db.get_system_llm_endpoint(str(endpoint_row["id"]))
        if (
            not endpoint
            or endpoint["label"] != label
            or endpoint["base_url"] != base_url
            or not isinstance(endpoint.get("api_key"), str)
            or not hmac.compare_digest(endpoint["api_key"], inference_key)
            or endpoint.get("transport_kind") is not None
        ):
            raise FixtureRefusal("A1 provider endpoint or key changed")
    endpoint_id = str(endpoint["id"])
    if existing_model is None:
        if await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM models WHERE model_id=$1)", model_id,
        ):
            raise FixtureRefusal("A1 model authority changed")
        await db.create_model(
            provider_kind="endpoint", provider_ref=endpoint_id,
            model_id=model_id, display_label=f"A1 deterministic model {run_id}",
            capabilities=["chat", "auxiliary"], family="e2e", source="ui",
        )
    model = await db.resolve_catalog_model(model_id, capability="chat")
    if (
        not model
        or model.get("provider_kind") != "endpoint"
        or model.get("provider_ref") != endpoint_id
        or model.get("endpoint_label") != label
        or model.get("endpoint_base_url") != base_url
        or not isinstance(model.get("api_key"), str)
        or not hmac.compare_digest(model["api_key"], inference_key)
        or set(model.get("capabilities") or []) != {"chat", "auxiliary"}
        or model.get("family") != "e2e"
        or model.get("display_label") != f"A1 deterministic model {run_id}"
    ):
        raise FixtureRefusal("A1 deterministic model changed")
    return endpoint_id


async def prepare_fixture(
    db: Any,
    provisioner: Any,
    *,
    run_id: str,
    namespace: str,
    vm_image: str,
    model_id: str,
) -> dict[str, str]:
    """Create/replay one paused owner Job, then enter the real VM preflight."""
    from orchestrator.services.manifest_execution_snapshot import (
        read_execution, srw_snapshot_config,
    )
    from orchestrator.services.vm_creation_preflight import _preflight
    from shared.vm_network_profile import NETWORK_PROFILE, compatible_image
    from shared.worker_queue import hold_worker_batch_for_preflight

    if (
        not _OWNED.fullmatch(run_id)
        or len(f"srw-a1-provider-{run_id}") > 63
        or namespace != run_id
        or os.environ.get("VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED") != "true"
        or os.environ.get("VM_CREATION_RETRY_ENABLED") != "true"
        or os.environ.get("VM_NETWORK_PROFILE_ENABLED") != "true"
        or provisioner.mode != "same-cluster"
        or not compatible_image(vm_image)
    ):
        raise FixtureRefusal("A1 fixture source is not exclusively enabled")
    if not re.fullmatch(r"e2e-vm-[a-z0-9-]{3,55}", model_id):
        raise FixtureRefusal("A1 deterministic model identity is malformed")
    model = await db.resolve_catalog_model(model_id, capability="chat")
    if (
        not model
        or model.get("provider_kind") != "endpoint"
        or model.get("endpoint_label") != f"srw-a1-provider-{run_id}"
        or model.get("endpoint_base_url")
        != f"http://srw-a1-provider.{namespace}.svc.cluster.local:8000/v1"
        or not isinstance(model.get("api_key"), str)
        or len(model["api_key"]) < 16
    ):
        raise FixtureRefusal("A1 deterministic model endpoint is unavailable")
    owner_id = uuid5(NAMESPACE_URL, f"srw-a1-owner:{run_id}")
    job_id = uuid5(NAMESPACE_URL, f"srw-a1-job:{run_id}")
    if await db.fetchval("SELECT EXISTS(SELECT 1 FROM jobs WHERE id<>$1)", job_id):
        raise FixtureRefusal("A1 fixture database contains another Job")
    if await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_kind='worker_batch' "
        "AND state IN ('queued','leased') AND unit_id<>$1)", job_id,
    ):
        raise FixtureRefusal("another worker batch is runnable")
    if await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id<>$1 "
        "AND state IN ('queued','reconciling','attention','cancel_requested'))",
        job_id,
    ):
        raise FixtureRefusal("another VM creation is unresolved")

    async with db.acquire() as conn, conn.transaction():
        owner = await conn.fetchrow(
            "SELECT id,display_name,is_approved,is_admin,can_use_vm "
            "FROM users WHERE id=$1 FOR UPDATE", owner_id,
        )
        if owner is None:
            await conn.execute(
                "INSERT INTO users(id,display_name,is_approved,is_admin,can_use_vm) "
                "VALUES ($1,$2,true,false,true)",
                owner_id, f"A1 retained Resume gate {run_id}",
            )
            for key, value in (
                ("vm_workspace", "true"),
                ("shell_tools", "true"),
                ("autonomy_ceiling", '"full"'),
            ):
                await conn.execute(
                    "INSERT INTO capability_grants(scope_kind,scope_id,key,value_json) "
                    "VALUES ('user',$1,$2,$3::jsonb)", owner_id, key, value,
                )
        elif (
            owner["display_name"] != f"A1 retained Resume gate {run_id}"
            or owner["is_approved"] is not True
            or owner["is_admin"] is not False
            or owner["can_use_vm"] is not True
        ):
            raise FixtureRefusal("A1 run owner identity changed")
        grants = await conn.fetch(
            "SELECT key,value_json FROM capability_grants WHERE scope_kind='user' "
            "AND scope_id=$1", owner_id,
        )
        if {
            row["key"]: json.loads(row["value_json"])
            if isinstance(row["value_json"], str) else row["value_json"]
            for row in grants
        } != {
            "vm_workspace": True,
            "shell_tools": True,
            "autonomy_ceiling": "full",
        } or len(grants) != 3:
            raise FixtureRefusal("A1 owner VM entitlement changed")

    expert_name = f"a1-{run_id}"
    expected_config = _expert_config(model_id)
    expert_row = await db.fetchrow(
        "SELECT id FROM experts WHERE name=$1 AND owner_id=$2",
        expert_name, owner_id,
    )
    expert = (
        await db.get_expert_by_id(str(expert_row["id"]))
        if expert_row is not None else None
    )
    if expert is None:
        created = await db.create_expert(
            name=expert_name, display_name=f"A1 expert {run_id}",
            expert_type="worker", owner_id=str(owner_id),
            config=expected_config,
            prompts={"persona": "Use only the run-owned deterministic provider."},
        )
        expert_id = UUID(str(created["id"]))
    else:
        if (
            expert["owner_id"] != owner_id
            or expert["expert_type"] != "worker"
            or _json(expert["config"]) != expected_config
        ):
            raise FixtureRefusal("A1 expert authority changed")
        expert_id = expert["id"]

    job = await db.fetchrow(
        "SELECT id,user_id,expert_id,status::text AS status,execution_lane,"
        "context,config_override FROM jobs WHERE id=$1", job_id,
    )
    override = {
        "workspace": {"backend": "vm", "vm": {
            "image": vm_image, "cpu_cores": 2,
            "memory": "2Gi", "disk_size": "12Gi",
        }},
    }
    if job is None:
        # The new paused row is invisible until its real execution snapshot and
        # closed queue tombstone have both committed. No worker can lease it.
        async with db.acquire() as conn, conn.transaction():
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id<>$1)", job_id,
            ):
                raise FixtureRefusal("A1 fixture database contains another Job")
            await db.create_job(
                description=f"E2E-{run_id}: retained sentinel worker",
                context={"vm_retained_resume_acceptance_gate": run_id},
                config_override=override,
                requested_workspace_backend="vm",
                status="paused", execution_lane="stateless",
                user_id=str(owner_id), expert_id=str(expert_id),
                origin="lifecycle", job_id=job_id, conn=conn,
            )
            await hold_worker_batch_for_preflight(
                conn, job_id=job_id, preserve_attempts=True,
            )
    elif (
        job["user_id"] != owner_id
        or job["expert_id"] != expert_id
        or job["status"] != "paused"
        or job["execution_lane"] != "stateless"
        or _json(job["context"]).get("vm_retained_resume_acceptance_gate") != run_id
        or _json(job["config_override"]) != override
    ):
        raise FixtureRefusal("A1 fixture Job identity changed")

    snapshot = await read_execution(db, "Job", str(job_id))
    if snapshot is None:
        raise FixtureRefusal("A1 fixture execution snapshot is unavailable")
    _, policy = srw_snapshot_config(snapshot)
    if (
        _json(_json(policy.get("workspace")).get("vm")).get("image") != vm_image
        or _json(policy.get("llm")).get("model") != model_id
        or snapshot.get("owner_id") != owner_id
    ):
        raise FixtureRefusal("A1 frozen image or model changed")
    queue = await db.fetchrow(
        "SELECT state,leased_by,lease_token FROM run_queue WHERE unit_id=$1 "
        "AND unit_kind='worker_batch'", job_id,
    )
    if not queue or (queue["state"], queue["leased_by"], queue["lease_token"]) != (
        "done", None, 0,
    ):
        raise FixtureRefusal("A1 fixture queue is not never-leased")
    if await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)",
        job_id,
    ):
        raise FixtureRefusal("A1 fixture has a worker attempt")

    row = await db.fetchrow("SELECT context FROM jobs WHERE id=$1", job_id)
    context = _json(row["context"])
    preflight = _preflight(_json(context.get("vm")))
    if preflight is None:
        ack = await provisioner.create_vm(
            str(job_id), cpu_cores=2, memory="2Gi", disk_size="12Gi",
            vm_image=vm_image,
        )
        if (
            not isinstance(ack, dict)
            or ack.get("creation_retry_protocol") != 1
            or ack.get("status") != "creation_pending"
            or ack.get("job_id") != str(job_id)
        ):
            raise FixtureRefusal("A1 real creation preflight was not admitted")
        row = await db.fetchrow("SELECT context FROM jobs WHERE id=$1", job_id)
        context = _json(row["context"])
        preflight = _preflight(_json(context.get("vm")))
    if (
        preflight is None
        or preflight["request"]["vm_image"] != vm_image
        or preflight["request"]["network_profile"] != NETWORK_PROFILE
        or preflight["request"]["job_id"] != str(job_id)
        or str(snapshot["id"]) != preflight["execution_id"]
        or snapshot["revision"] != preflight["execution_revision"]
        or snapshot["generation"] != preflight["execution_generation"]
    ):
        raise FixtureRefusal("A1 real creation request changed frozen authority")
    return {
        "job_id": str(job_id), "owner_id": str(owner_id),
        "expert_id": str(expert_id), "request_id": preflight["request_id"],
        "provision_generation": preflight["request"]["provision_generation"],
    }


async def probe_ready_fixture(
    db: Any, provisioner: Any, *, run_id: str, vm_image: str,
    prepared: dict[str, str],
) -> dict[str, str] | None:
    """Read authenticated controller state; never manufacture Ready authority."""
    from orchestrator.services.vm_creation_preflight import _preflight
    from shared.vm_creation_retry import canonical_request_digest
    from shared.vm_network_profile import NETWORK_PROFILE, reusable_profile_evidence

    job_id = UUID(prepared["job_id"])
    row = await db.fetchrow(
        "SELECT id,user_id,status::text AS status,execution_lane,"
        "assigned_agent_id,context,config_override FROM jobs WHERE id=$1", job_id,
    )
    if not row:
        raise FixtureRefusal("A1 fixture Job disappeared")
    context = _json(row["context"])
    vm = _json(context.get("vm"))
    workspace = _json(_json(row["config_override"]).get("workspace"))
    preflight = _preflight(vm)
    if (
        str(row["user_id"]) != prepared["owner_id"]
        or row["status"] != "paused"
        or row["execution_lane"] != "stateless"
        or row["assigned_agent_id"] is not None
        or context.get("vm_retained_resume_acceptance_gate") != run_id
        or any(context.get(key) is not None for key in (
            "_workspace_dispatch_authority", "_completion_control_claim",
            "_stateless_control_claim", "_operator_pause_hold",
        ))
        or workspace.get("backend") != "vm"
        or _json(workspace.get("vm")).get("image") != vm_image
        or preflight is None
        or preflight["request_id"] != prepared["request_id"]
        or preflight["request"]["provision_generation"]
        != prepared["provision_generation"]
        or preflight["request"].get("network_profile") != NETWORK_PROFILE
    ):
        raise FixtureRefusal("A1 fixture immutable authority changed")
    if vm.get("status") != "ready":
        return None
    status = _json(await provisioner.query_status(str(job_id), timeout=10))
    identity = {
        "vm_uid": vm.get("vm_uid"),
        "vmi_uid": _json(_json(vm.get("provisioning")).get("identity")).get("vmi_uid"),
        "launcher_uid": vm.get("active_pod_uid"),
        "pvc_uid": vm.get("rootdisk_pvc_uid"),
        "pod_ip": vm.get("pod_ip") or status.get("pod_ip"),
        "ssh_host_key_fingerprint": vm.get("ssh_host_key_fingerprint")
        or status.get("ssh_host_key_fingerprint"),
    }
    if (
        status.get("ready") is not True
        or vm.get("status") != "ready"
        or vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation")
        != prepared["provision_generation"]
        or vm.get("creation_request_id") != prepared["request_id"]
        or vm.get("provision_generation") != prepared["provision_generation"]
        or identity["vm_uid"] != status.get("vm_uid")
        or identity["vmi_uid"] != _json(status.get("provisioning")).get("vmi_uid")
        or identity["launcher_uid"] != status.get("active_pod_uid")
        or identity["pvc_uid"] != status.get("rootdisk_pvc_uid")
        or not identity["pod_ip"]
        or not identity["ssh_host_key_fingerprint"]
        or not reusable_profile_evidence(
            vm.get("network_profile_evidence"), NETWORK_PROFILE,
            provision_generation=prepared["provision_generation"],
            vm_uid=identity["vm_uid"], pvc_uid=identity["pvc_uid"],
            vmi_uid=identity["vmi_uid"], launcher_uid=identity["launcher_uid"],
        )
    ):
        return None
    for key in ("vm_uid", "vmi_uid", "launcher_uid", "pvc_uid"):
        try:
            if str(UUID(str(identity[key]))) != identity[key]:
                return None
        except (TypeError, ValueError):
            return None
    retry = await db.fetchrow(
        "SELECT request_id,job_id,provision_generation,state,reason,ready_at,"
        "canonical_request,request_digest,observed_vm_uid,observed_pvc_uid,"
        "execution_id,execution_revision,execution_generation "
        "FROM vm_creation_retries WHERE request_id=$1",
        UUID(prepared["request_id"]),
    )
    if not retry:
        return None
    canonical = _json(retry["canonical_request"])
    if (
        str(retry["request_id"]) != prepared["request_id"]
        or retry["job_id"] != job_id
        or str(retry["provision_generation"])
        != prepared["provision_generation"]
        or retry["state"] != "succeeded"
        or retry["reason"] != "creation_adopted"
        or str(retry["observed_vm_uid"]) != identity["vm_uid"]
        or str(retry["observed_pvc_uid"]) != identity["pvc_uid"]
        or canonical != preflight["request"]
        or canonical_request_digest(canonical) != retry["request_digest"]
        or str(retry["execution_id"]) != preflight["execution_id"]
        or retry["execution_revision"] != preflight["execution_revision"]
        or retry["execution_generation"] != preflight["execution_generation"]
    ):
        return None
    queue = await db.fetchrow(
        "SELECT state,leased_by,lease_token FROM run_queue "
        "WHERE unit_kind='worker_batch' AND unit_id=$1", job_id,
    )
    if not queue or (queue["state"], queue["leased_by"], queue["lease_token"]) != (
        "done", None, 0,
    ) or await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)", job_id,
    ):
        raise FixtureRefusal("A1 fixture acquired a worker before acceptance")
    if retry["ready_at"] is None:
        from orchestrator.operator_cli.vm_fixture_readiness import (
            settle_owned_fixture_ready,
        )

        if context.get("_vm_creation_pending") != prepared["request_id"]:
            return None
        if not await settle_owned_fixture_ready(
            db=db, job_id=str(job_id), owner_id=prepared["owner_id"],
            run_id=run_id, marker_key="vm_retained_resume_acceptance_gate",
            request_id=prepared["request_id"],
            generation=prepared["provision_generation"],
            vm_uid=identity["vm_uid"], pvc_uid=identity["pvc_uid"],
        ):
            return None
        # The production writer has changed only the pending/Ready receipt.
        # Re-read every live, source and queue predicate before returning it.
        return await probe_ready_fixture(
            db, provisioner, run_id=run_id, vm_image=vm_image,
            prepared=prepared,
        )
    if context.get("_vm_creation_pending") is not None:
        return None
    return {key: str(value) for key, value in identity.items()}


async def wait_ready_fixture(
    db: Any, provisioner: Any, core: Any, *, run_id: str,
    namespace: str, vm_image: str, prepared: dict[str, str],
    timeout_seconds: int = 900,
) -> dict[str, str]:
    """Wait for real first-boot proof, then bind the retained PVC to its PV."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        identity = await probe_ready_fixture(
            db, provisioner, run_id=run_id, vm_image=vm_image,
            prepared=prepared,
        )
        if identity is not None:
            pvc = await asyncio.to_thread(
                core.read_namespaced_persistent_volume_claim,
                f"agent-vm-{prepared['job_id']}-rootdisk", namespace,
            )
            if str(pvc.metadata.uid) != identity["pvc_uid"]:
                raise FixtureRefusal("A1 retained PVC UID changed")
            if pvc.spec.volume_name:
                pv = await asyncio.to_thread(
                    core.read_persistent_volume, pvc.spec.volume_name,
                )
                if (
                    pv.metadata.uid
                    and pv.spec.claim_ref.uid == pvc.metadata.uid
                ):
                    return {**identity, "pv_uid": str(pv.metadata.uid),
                            "pv_name": pvc.spec.volume_name}
        await asyncio.sleep(2)
    raise FixtureRefusal("A1 first-boot Ready proof did not arrive")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--vm-image", required=True)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from kubernetes import client, config
    from orchestrator.database.postgres import PostgresDB
    from orchestrator.operator_cli.vm_retained_resume_acceptance import _atomic_result
    from orchestrator.services.vm_provisioner import VMProvisioner

    if (
        args.confirm != "disposable-vm-retained-resume-fixture-v1"
        or not _OWNED.fullmatch(args.run_id)
        or args.namespace != args.run_id
        or os.environ.get("VM_NAMESPACE", args.namespace) != args.namespace
        or Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .read_text(encoding="utf-8").strip() != args.namespace
        or args.output.parent != Path(
            f"/tmp/srw-vm-retained-resume-gate/{args.run_id}"
        )
        or args.output.name != "fixture.json"
    ):
        raise FixtureRefusal("A1 in-image fixture scope is not exact")
    db = PostgresDB(min_connections=1, max_connections=4)
    provisioner = VMProvisioner()
    api_client = None
    try:
        await db.connect()
        provisioner.connect(db)
        config.load_incluster_config()
        api_client = client.ApiClient()
        core = client.CoreV1Api(api_client)
        inference_key = sys.stdin.readline(258).strip()
        await seed_model(
            db, run_id=args.run_id, namespace=args.namespace,
            model_id=args.model_id, inference_key=inference_key,
        )
        prepared = await prepare_fixture(
            db, provisioner, run_id=args.run_id, namespace=args.namespace,
            vm_image=args.vm_image, model_id=args.model_id,
        )
        ready = await wait_ready_fixture(
            db, provisioner, core, run_id=args.run_id,
            namespace=args.namespace, vm_image=args.vm_image,
            prepared=prepared,
        )
        _atomic_result(args.output, {
            "protocol_version": 1, "run_id": args.run_id,
            "outcome": "ready", **prepared, **ready,
        })
        return 0
    except Exception as exc:
        # Database, Kubernetes, and controller errors may contain credentials.
        _atomic_result(args.output, {
            "protocol_version": 1, "run_id": args.run_id,
            "outcome": "held", "error_class": type(exc).__name__,
        })
        return 1
    finally:
        await provisioner.disconnect()
        await db.close()
        if api_client is not None:
            api_client.close()


def main() -> int:
    return asyncio.run(_async_main(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
