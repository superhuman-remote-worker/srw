"""Trusted phone jobs; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

from orchestrator.services.agent_pod_entrypoint import validate_config_name
from orchestrator.services.datasource_policy import default_datasource_selection
from orchestrator.services.telephony.store import LINE_DISPATCH_FIELDS, TelephonyStore
from shared.runtime.core.loader import canonical_config_name


async def create_phone_job(store: TelephonyStore, call: dict, line: dict) -> dict:
    db = store.db
    owner_id, project_id = str(line["owner_user_id"]), str(line["project_id"])
    validate_config_name(line["expert"])
    datasource_ids, revisions = await default_datasource_selection(
        db, owner_id, [project_id], "none", execution_kind="job", unattended=True
    )
    context = {
        "telephony": {
            "call_id": str(call["id"]),
            "session_id": call["session_id"],
            "line_id": str(line["id"]),
            "line_name": line["name"],
            "direction": "inbound",
            "from_number": call["from_number"],
            "to_number": call["to_number"],
        }
    }
    # Publish the job and its reverse binding in one transaction: a concurrent
    # dispatcher must never inject credentials for an as-yet unbound call.
    async with db.acquire() as conn, conn.transaction():
        job = await db.create_job(
            origin="telephony",
            execution_lane="pinned",
            description=f'Phone call on line "{line["name"]}" from {call["from_number"]}. '
            "The call is already connected: a phone subagent handles it. "
            "Answer its questions while the call lasts, then process the transcript and any follow-ups.",
            user_id=owner_id,
            project_id=project_id,
            config_name=canonical_config_name(line["expert"]),
            config_override={"workspace": {"backend": "none"}},
            context=context,
            datasource_ids=datasource_ids,
            datasource_policy_revisions=revisions,
            datasource_selection_provenance={
                "origin": "default",
                "creation_path": "telephony",
                "policy_revisions": revisions,
            },
            authority_user_id=owner_id,
            authority_project_ids=[project_id],
            conn=conn,
        )
        await store.bind_job(conn, call["id"], job["id"])
    return job


async def dispatch_config(db, job: dict) -> dict | None:
    import json

    context = job.get("context") or {}
    if isinstance(context, str):
        context = json.loads(context)
    call_id = (context.get("telephony") or {}).get("call_id")
    if not call_id:
        return None
    store = TelephonyStore(db)
    call = await store.get_call(call_id, job_id=job["id"])
    if not call:
        raise ValueError("Phone call is not bound to this job")
    settings = await store.get_settings(call["settings_id"])
    line = await store.get_line(call["line_id"]) if call["line_id"] else None
    if line is None:
        line = (call.get("metadata") or {}).get("line")
    if not settings or not line:
        raise ValueError("Phone call configuration is unavailable")
    return {
        "call_id": str(call["id"]),
        "session_id": call["session_id"],
        "api_key": await store.api_key(settings),
        "api_base": settings["api_base"],
        "ws_base": settings["ws_base"],
        "extra_headers": settings["extra_headers"],
        "line": {
            key: str(line[key]) if key == "id" else line[key]
            for key in LINE_DISPATCH_FIELDS
        },
    }
