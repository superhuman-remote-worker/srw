"""Cloud-only execution of an idle thread's durable pending push.

No PersistentSession, transcript, shell claim, pull, or LLM is constructed.
The backend is used solely to read the already committed workspace bytes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from shared.cloud_push_tasks import (
    abandon_bg_push,
    adopt_bg_push,
    bg_push_lease_is_current,
)
from agent.api.lease_context import LeaseLostError

logger = logging.getLogger(__name__)


def build_push_backend(workspace: dict, *, thread_id: str):
    backend = workspace["backend"]
    if backend == "virtual":
        from agent.core.backends.factory import create_lite_backend

        # The mirror pushes the workspace prefix; a Session's cloud folders
        # (further mount entries) are the destination, never the source.
        return create_lite_backend(
            SimpleNamespace(**workspace), job_id=thread_id, cloud_folders=False
        )
    if backend != "sandbox":
        raise ValueError("background push requires a durable stateless workspace")
    from shared.runtime.core.backends.remote import RemoteBackend

    for key in (
        "host",
        "workspace_generation",
        "runtime_incarnation",
        "host_key_fingerprint",
    ):
        if not workspace.get(key):
            raise ValueError("background push lacks attested workspace identity")
    return RemoteBackend(
        host=workspace["host"],
        port=workspace.get("port") or 30022,
        username="agent-host",
        key_path=workspace.get("key_path") or "/run/secrets/vm-ssh-key",
        workspace_path="/home/agent-host/workspace",
        job_id=thread_id,
        workspace_generation=workspace["workspace_generation"],
        runtime_incarnation=workspace["runtime_incarnation"],
        expected_host_key_fingerprint=workspace["host_key_fingerprint"],
    )


async def run_adopted_cloud_push(
    db: Any, claim, bundle: dict, *, pod_name: str, pod_uid: str
) -> None:
    from agent.api.persistent_app import (
        _CloudGenerationClaim,
        _CloudWriterFence,
        _PushProgressRecorder,
        _ack_cloud_generation,
        _assert_cloud_generation_owner,
        _build_sync_coordinator,
        _push_owner_heartbeat_loop,
    )

    if bundle.get("obsolete") is True:
        return
    if bundle.get("unit_kind") != "bg_task" or bundle.get("task_kind") != "cloud_push":
        raise ValueError("unexpected background task bundle")
    thread_id = str(bundle["thread_id"])
    workspace = bundle["workspace"]
    generation = str(workspace["workspace_generation"])
    async with db.acquire() as conn:
        requirements = await adopt_bg_push(
            conn,
            unit_id=claim.unit_id,
            lease_token=claim.lease_token,
            pod_name=pod_name,
            pod_uid=pod_uid,
            workspace_generation=generation,
        )
    if not requirements:
        return
    token = next(iter(requirements.values())).push_owner_token
    owner = _CloudGenerationClaim(
        thread_id=thread_id,
        lease_token=claim.lease_token,
        workspace_generation=generation,
        postgres=db,
        lease_handle=None,
        fence=_CloudWriterFence(kind="push", push_owner_token=token),
    )
    heartbeat = asyncio.create_task(_push_owner_heartbeat_loop(owner))
    backend = None
    sync = None
    recorder = _PushProgressRecorder(owner)
    succeeded = False

    async def before_write():
        if not await bg_push_lease_is_current(
            db, unit_id=claim.unit_id, lease_token=claim.lease_token, pod_name=pod_name
        ):
            raise LeaseLostError("background push queue lease lost")
        await _assert_cloud_generation_owner(owner)

    async def acknowledge(mount_id, requirement):
        # Flush BEFORE acknowledgement makes the pending-row CAS disappear.
        await recorder.flush()
        await before_write()
        await _ack_cloud_generation(owner, mount_id, requirement)

    async def progress():
        return {key: req.push_progress for key, req in requirements.items()}

    try:
        await before_write()
        backend = build_push_backend(workspace, thread_id=thread_id)
        await asyncio.to_thread(backend.connect)
        sync = _build_sync_coordinator(
            workspace_path=Path(backend.root),
            workspace_backend=backend,
            cloud_cfg=bundle["cloud_sync"],
            thread_id=thread_id,
            workspace_generation=generation,
        )
        if sync is None:
            raise RuntimeError("background push has no configured cloud mounts")
        logger.info(
            "background cloud push adopted: thread=%s unit=%s token=%d resumed_files=%d",
            thread_id,
            claim.unit_id,
            token,
            sum(
                len(req.push_progress.get("files", {})) for req in requirements.values()
            ),
        )
        await sync.reconcile_before_pull(
            requirements,
            before_write=before_write,
            acknowledge=acknowledge,
            adopt=progress,
            progress=recorder.record,
        )
        succeeded = True
        logger.info(
            "background cloud push acknowledged: thread=%s unit=%s",
            thread_id,
            claim.unit_id,
        )
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
        try:
            if not succeeded:
                await recorder.flush()
                await abandon_bg_push(db, thread_id=thread_id, push_owner_token=token)
        finally:
            try:
                if sync is not None:
                    await sync.aclose()
            finally:
                if backend is not None:
                    await asyncio.to_thread(backend.disconnect)
