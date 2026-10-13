"""Set up a phone line from inside the orchestrator container.

Run ``python -m orchestrator.operator_cli.telephony_line --help`` in the
orchestrator pod (it uses the pod's database settings and APP_ENCRYPTION_KEY):

* ``settings`` upserts the ``telephony_settings`` row for one OpenAI project by
  ``--name``. The webhook signing secret (``whsec_…``) is read from the first
  stdin line, an optional project API key from the second (empty: use the
  system OpenAI key). Secrets are never taken from arguments.
* ``line`` upserts a ``telephony_lines`` row by ``--name`` from a JSON object
  on stdin (persona, greeting, fact_sheet, number, voice, …), bound to a
  settings row, an SRW project and the user its jobs run as.
* ``show`` lists settings and lines without secrets.

The HTTP admin API (``/api/telephony/*``) does the same for a signed-in
administrator; personal access tokens are refused there by design. Design:
knowledge-base/knowledge/features/phone_calls_as_subagents.md (§7.1 P2, P6).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Optional, TextIO
from uuid import UUID

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.telephony.store import (
    LINE_FIELDS,
    TelephonyStore,
    settings_view,
)

_LINE_JSON_FIELDS = LINE_FIELDS - {"settings_id", "project_id", "owner_user_id", "name"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="operation", required=True)

    settings = commands.add_parser(
        "settings", help="Upsert the settings of one OpenAI project."
    )
    settings.add_argument("--name", required=True)
    settings.add_argument("--openai-project-id", required=True)
    settings.add_argument("--project-id", help="SRW project the lines default to")
    settings.add_argument("--api-base", help="Default https://api.openai.com/v1")
    settings.add_argument("--ws-base", help="Default wss://api.openai.com/v1")
    settings.add_argument("--disabled", action="store_true")

    line = commands.add_parser("line", help="Upsert a phone line from JSON on stdin.")
    line.add_argument("--name", required=True)
    line.add_argument("--settings-name", required=True)
    line.add_argument("--project-id", required=True)
    owner = line.add_mutually_exclusive_group(required=True)
    owner.add_argument("--owner-email")
    owner.add_argument("--owner-id")

    commands.add_parser("show", help="List settings and lines without secrets.")
    return parser


async def _row_id(db: PostgresDB, table: str, name: str) -> Optional[UUID]:
    async with db.acquire() as conn:
        return await conn.fetchval(f"SELECT id FROM {table} WHERE name=$1", name)


async def upsert_settings(
    db: PostgresDB, args: argparse.Namespace, stdin: TextIO
) -> dict[str, Any]:
    lines = stdin.read().splitlines()
    secret = (lines[0] if lines else "").strip()
    api_key = (lines[1] if len(lines) > 1 else "").strip()
    existing = await _row_id(db, "telephony_settings", args.name)
    if not secret and existing is None:
        raise SystemExit(
            "the webhook signing secret (first stdin line) is required for new settings"
        )
    values: dict[str, Any] = {
        "name": args.name,
        "openai_project_id": args.openai_project_id,
        "enabled": not args.disabled,
    }
    if secret:
        if not secret.startswith("whsec_"):
            raise SystemExit("the webhook signing secret must start with whsec_")
        values["webhook_secret"] = secret
    if api_key:
        values["api_key"] = api_key
    for key in ("project_id", "api_base", "ws_base"):
        value = getattr(args, key)
        if value:
            values[key] = value
    row = await TelephonyStore(db).save("telephony_settings", values, existing)
    return settings_view(row)


async def upsert_line(
    db: PostgresDB, args: argparse.Namespace, stdin: TextIO
) -> dict[str, Any]:
    payload = json.loads(stdin.read() or "{}")
    if not isinstance(payload, dict):
        raise SystemExit("stdin must be one JSON object")
    unknown = set(payload) - _LINE_JSON_FIELDS
    if unknown:
        raise SystemExit(f"unknown line fields: {', '.join(sorted(unknown))}")
    settings_id = await _row_id(db, "telephony_settings", args.settings_name)
    if settings_id is None:
        raise SystemExit(f"no telephony settings named {args.settings_name!r}")
    owner_id = args.owner_id
    if args.owner_email:
        async with db.acquire() as conn:
            owner_id = await conn.fetchval(
                "SELECT id FROM users WHERE email=$1", args.owner_email
            )
        if owner_id is None:
            raise SystemExit(f"no user with email {args.owner_email!r}")
    values = {
        **payload,
        "name": args.name,
        "settings_id": settings_id,
        "project_id": args.project_id,
        "owner_user_id": owner_id,
    }
    existing = await _row_id(db, "telephony_lines", args.name)
    row = await TelephonyStore(db).save("telephony_lines", values, existing)
    return {
        key: (str(value) if isinstance(value, UUID) else value)
        for key, value in row.items()
        if key
        in (
            "id",
            "name",
            "number",
            "is_default",
            "enabled",
            "expert",
            "voice",
            "settings_id",
            "project_id",
            "owner_user_id",
        )
    }


async def show(db: PostgresDB) -> dict[str, Any]:
    store = TelephonyStore(db)
    settings = [settings_view(row) for row in await store.settings()]
    lines = [
        {
            "id": str(row["id"]),
            "name": row["name"],
            "number": row["number"],
            "is_default": row["is_default"],
            "enabled": row["enabled"],
            "expert": row["expert"],
            "voice": row["voice"],
            "settings_id": str(row["settings_id"]),
            "fact_sheet_chars": len(row.get("fact_sheet") or ""),
        }
        for row in await store.lines()
    ]
    return {"settings": settings, "lines": lines}


async def run(
    argv: list[str], stdin: TextIO = sys.stdin, db: Optional[PostgresDB] = None
) -> dict[str, Any]:
    args = _parser().parse_args(argv)
    owned = db is None
    if db is None:
        db = PostgresDB(min_connections=1, max_connections=2)
        await db.connect()
    try:
        if args.operation == "settings":
            return await upsert_settings(db, args, stdin)
        if args.operation == "line":
            return await upsert_line(db, args, stdin)
        return await show(db)
    finally:
        if owned:
            await db.close()


def main() -> int:
    result = asyncio.run(run(sys.argv[1:]))
    print(json.dumps(result, indent=2, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
