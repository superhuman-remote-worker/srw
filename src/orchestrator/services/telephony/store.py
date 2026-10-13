"""Phone persistence; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from orchestrator.database.postgres import _decrypt_stored, _encrypt_optional
from shared.runtime.telephony.webhook import sip_uri_user

EVENT_KINDS = frozenset(
    "transcript delegation answer instruction thinking fallback question follow_up "
    "transfer hangup dtmf usage closed error system".split()
)
CALL_FIELDS = (
    "id session_id line_id line_name direction status from_number to_number "
    "created_at accepted_at attached_at ended_at close_reason usage_seconds job_id"
).split()
SETTINGS_FIELDS = frozenset(
    "name project_id openai_project_id api_base ws_base extra_headers enabled "
    "webhook_secret api_key".split()
)
LINE_FIELDS = frozenset(
    "settings_id project_id owner_user_id name number is_default enabled engine model "
    "voice expert persona greeting fact_sheet transfer_target max_duration_s "
    "answer_timeout_s no_job_fallback_s goodbye".split()
)
LINE_DISPATCH_FIELDS = (
    "id",
    "name",
    "max_duration_s",
    "answer_timeout_s",
    "transfer_target",
    "goodbye",
)
CALL_SELECT = "SELECT c.*, COALESCE(l.name, c.metadata #>> '{line,name}') AS line_name FROM calls c LEFT JOIN telephony_lines l ON l.id=c.line_id"


def record(row) -> dict | None:
    if row is None:
        return None
    result = dict(row)
    for key in ("extra_headers", "metadata", "payload", "sip_headers"):
        if isinstance(result.get(key), str):
            result[key] = json.loads(result[key])
    return result


def call_view(call: dict) -> dict:
    return {key: call.get(key) for key in CALL_FIELDS}


def settings_view(settings: dict) -> dict:
    result = {k: v for k, v in settings.items() if not k.endswith("_encrypted")}
    result["webhook_secret_set"] = bool(settings.get("webhook_secret_encrypted"))
    result["api_key_set"] = bool(settings.get("api_key_encrypted"))
    return result


class TelephonyStore:
    def __init__(self, db: Any):
        self.db = db

    async def settings(self, *, enabled_only=False) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM telephony_settings"
                + (" WHERE enabled" if enabled_only else "")
                + " ORDER BY created_at, id"
            )
        return [record(row) for row in rows]

    async def get_settings(self, settings_id) -> dict | None:
        async with self.db.acquire() as conn:
            return record(
                await conn.fetchrow(
                    "SELECT * FROM telephony_settings WHERE id=$1",
                    UUID(str(settings_id)),
                )
            )

    async def lines(self, settings_id=None, *, enabled_only=False) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM telephony_lines WHERE ($1::uuid IS NULL OR settings_id=$1)"
                + (" AND enabled" if enabled_only else "")
                + " ORDER BY created_at, id",
                UUID(str(settings_id)) if settings_id else None,
            )
        return [record(row) for row in rows]

    async def get_line(self, line_id) -> dict | None:
        async with self.db.acquire() as conn:
            return record(
                await conn.fetchrow(
                    "SELECT * FROM telephony_lines WHERE id=$1", UUID(str(line_id))
                )
            )

    async def save(self, table: str, values: dict, row_id=None) -> dict | None:
        allowed = SETTINGS_FIELDS if table == "telephony_settings" else LINE_FIELDS
        if (
            table not in {"telephony_settings", "telephony_lines"}
            or set(values) - allowed
        ):
            raise ValueError("Unsupported phone configuration fields")
        data = dict(values)
        for key in ("webhook_secret", "api_key"):
            if key in data:
                data[key + "_encrypted"] = _encrypt_optional(data.pop(key))
        for key, value in list(data.items()):
            if key.endswith("_id") and key != "openai_project_id":
                data[key] = UUID(str(value)) if value else None
            elif key == "extra_headers":
                data[key] = json.dumps(value)
        async with self.db.acquire() as conn:
            if row_id:
                if not data:
                    return record(
                        await conn.fetchrow(
                            f"SELECT * FROM {table} WHERE id=$1", UUID(str(row_id))
                        )
                    )
                assignments = ", ".join(f"{key}=${i}" for i, key in enumerate(data, 2))
                row = await conn.fetchrow(
                    f"UPDATE {table} SET {assignments}, updated_at=now() WHERE id=$1 RETURNING *",
                    UUID(str(row_id)),
                    *data.values(),
                )
            else:
                columns = ", ".join(data)
                placeholders = ", ".join(f"${i}" for i in range(1, len(data) + 1))
                row = await conn.fetchrow(
                    f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *",
                    *data.values(),
                )
        return record(row)

    async def delete(self, table: str, row_id) -> bool:
        if table not in {"telephony_settings", "telephony_lines"}:
            raise ValueError("Unsupported phone table")
        async with self.db.acquire() as conn, conn.transaction():
            # Lock before checking calls so an accept cannot select a line while
            # its deletion passes the active-call check.
            row = await conn.fetchrow(
                f"SELECT id FROM {table} WHERE id=$1 FOR UPDATE", UUID(str(row_id))
            )
            if row is None:
                return False
            if table == "telephony_lines" and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM calls WHERE line_id=$1 AND status IN ('ringing','active'))",
                UUID(str(row_id)),
            ):
                raise ValueError(
                    "Disable the line until its calls end before deleting it"
                )
            await conn.execute(f"DELETE FROM {table} WHERE id=$1", UUID(str(row_id)))
            return True

    async def api_key(self, settings: dict) -> str:
        encrypted = settings.get("api_key_encrypted")
        key = (
            _decrypt_stored(encrypted, field="telephony_settings.api_key")
            if encrypted
            else await self.db.get_system_api_key("openai")
        )
        if not key:
            raise ValueError("No usable system or telephony OpenAI key")
        return key

    async def insert_incoming(self, settings_id, incoming) -> dict | None:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO calls(session_id, webhook_id, settings_id, direction,
                    status, from_number, to_number, sip_headers)
                VALUES($1,$2,$3,'inbound','ringing',$4,$5,$6::jsonb)
                ON CONFLICT DO NOTHING RETURNING *""",
                incoming.session_id,
                incoming.webhook_id,
                UUID(str(settings_id)),
                sip_uri_user(incoming.sip_header("From")),
                sip_uri_user(incoming.sip_header("To")),
                json.dumps([{"name": k, "value": v} for k, v in incoming.sip_headers]),
            )
        return record(row)

    async def get_call(self, call_id, *, job_id=None) -> dict | None:
        async with self.db.acquire() as conn:
            return record(
                await conn.fetchrow(
                    CALL_SELECT
                    + " WHERE c.id=$1 AND ($2::uuid IS NULL OR c.job_id=$2)",
                    UUID(str(call_id)),
                    UUID(str(job_id)) if job_id else None,
                )
            )

    async def job_calls(self, job_id) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                CALL_SELECT + " WHERE c.job_id=$1 ORDER BY c.created_at, c.id",
                UUID(str(job_id)),
            )
        return [call_view(record(row)) for row in rows]

    async def set_line(self, call_id, line: dict) -> None:
        async with self.db.acquire() as conn:
            snapshot = {
                key: str(line[key]) if key == "id" else line[key]
                for key in LINE_DISPATCH_FIELDS
            }
            await conn.execute(
                "UPDATE calls SET line_id=$2, project_id=$3, metadata=metadata || $4::jsonb WHERE id=$1 AND status='ringing'",
                UUID(str(call_id)),
                line["id"],
                line["project_id"],
                json.dumps({"line": snapshot}),
            )

    async def activate(self, call_id) -> None:
        async with self.db.acquire() as conn:
            await conn.execute(
                "UPDATE calls SET status='active', accepted_at=now() WHERE id=$1 AND status='ringing'",
                UUID(str(call_id)),
            )

    async def fail(self, call_id, status: str, error: str) -> None:
        async with self.db.acquire() as conn:
            await conn.execute(
                "UPDATE calls SET status=$2, ended_at=now(), error=$3 WHERE id=$1 AND status='ringing'",
                UUID(str(call_id)),
                status,
                error,
            )

    async def bind_job(self, conn, call_id, job_id) -> None:
        await conn.execute(
            "UPDATE calls SET job_id=$2 WHERE id=$1 AND job_id IS NULL",
            UUID(str(call_id)),
            UUID(str(job_id)),
        )

    async def attach_actor(self, call_id, job_id) -> dict | None:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE calls SET attached_at=COALESCE(attached_at,now()) WHERE id=$1 AND job_id=$2 RETURNING id",
                UUID(str(call_id)),
                UUID(str(job_id)),
            )
        return await self.get_call(call_id) if row else None

    async def claim(self, owner: str, call_id=None) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """UPDATE calls SET recorder_owner=$1, recorder_lease_until=now()+interval '30 seconds'
                WHERE status='active' AND (recorder_lease_until IS NULL OR recorder_lease_until < now())
                AND ($2::uuid IS NULL OR id=$2) RETURNING *""",
                owner,
                UUID(str(call_id)) if call_id else None,
            )
        return [record(row) for row in rows]

    async def renew(self, call_id, owner: str) -> bool:
        async with self.db.acquire() as conn:
            return bool(
                await conn.fetchval(
                    """UPDATE calls SET recorder_lease_until=now()+interval '30 seconds'
                WHERE id=$1 AND recorder_owner=$2 AND recorder_lease_until>now()
                AND status='active' RETURNING id""",
                    UUID(str(call_id)),
                    owner,
                )
            )

    async def release(self, call_id, owner: str) -> None:
        async with self.db.acquire() as conn:
            await conn.execute(
                "UPDATE calls SET recorder_owner=NULL, recorder_lease_until=NULL WHERE id=$1 AND recorder_owner=$2",
                UUID(str(call_id)),
                owner,
            )

    async def owns(self, call_id, owner: str) -> bool:
        async with self.db.acquire() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM calls WHERE id=$1 AND recorder_owner=$2 AND recorder_lease_until>now() AND status='active')",
                    UUID(str(call_id)),
                    owner,
                )
            )

    @asynccontextmanager
    async def recorder_action(self, call_id, owner: str, *, unattached=False):
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """SELECT * FROM calls WHERE id=$1 AND recorder_owner=$2
                AND recorder_lease_until>now() AND status='active'
                AND (NOT $3::boolean OR attached_at IS NULL) FOR UPDATE""",
                UUID(str(call_id)),
                owner,
                unattached,
            )
            yield conn if row else None

    async def events(self, call_id, after_id=0, limit=500) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM call_events WHERE call_id=$1 AND id>$2 ORDER BY id LIMIT $3",
                UUID(str(call_id)),
                after_id,
                limit,
            )
        return [
            {k: v for k, v in record(row).items() if k != "event_id"} for row in rows
        ]

    async def has_event(self, conn, call_id, event_id) -> bool:
        return bool(
            await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM call_events WHERE call_id=$1 AND event_id=$2)",
                UUID(str(call_id)),
                event_id,
            )
        )

    async def append_event(self, call_id, event: dict, *, conn=None) -> bool:
        if event["kind"] not in EVENT_KINDS:
            raise ValueError("Unsupported call event kind")
        if conn is None:
            async with self.db.acquire() as acquired:
                return await self.append_event(call_id, event, conn=acquired)
        return bool(
            await conn.fetchval(
                """INSERT INTO call_events(call_id,event_id,kind,speaker,text,start_ms,end_ms,delegation_id,payload)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
            ON CONFLICT (call_id,event_id) WHERE event_id IS NOT NULL DO NOTHING RETURNING id""",
                UUID(str(call_id)),
                event.get("event_id"),
                event["kind"],
                event.get("speaker"),
                event.get("text"),
                event.get("start_ms"),
                event.get("end_ms"),
                event.get("delegation_id"),
                json.dumps(event.get("payload") or {}),
            )
        )

    async def write_events(self, call_id, events: list[dict]) -> int:
        async with self.db.acquire() as conn, conn.transaction():
            exists = await conn.fetchval(
                "SELECT id FROM calls WHERE id=$1 FOR KEY SHARE", UUID(str(call_id))
            )
            if not exists:
                raise LookupError("Call not found")
            return sum(
                [await self.append_event(call_id, event, conn=conn) for event in events]
            )

    async def pending_delegations(self, call_id, delay_s) -> list[dict]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """SELECT DISTINCT ON (d.delegation_id) d.* FROM call_events d
                WHERE d.call_id=$1 AND d.kind='delegation'
                AND d.created_at<=now()-$2*interval '1 second'
                AND NOT EXISTS(SELECT 1 FROM call_events f WHERE f.call_id=d.call_id
                    AND f.kind='fallback' AND f.delegation_id=d.delegation_id)
                ORDER BY d.delegation_id,d.id""",
                UUID(str(call_id)),
                float(delay_s),
            )
        return [record(row) for row in rows]

    async def usage(self, conn, call_id, seconds) -> None:
        await conn.execute(
            "UPDATE calls SET usage_seconds=$2 WHERE id=$1", UUID(str(call_id)), seconds
        )

    async def end(self, conn, call_id, reason: str, seconds=None) -> None:
        await conn.execute(
            "UPDATE calls SET status='ended', ended_at=now(), close_reason=$2, usage_seconds=COALESCE($3,usage_seconds) WHERE id=$1 AND status='active'",
            UUID(str(call_id)),
            reason,
            seconds,
        )
