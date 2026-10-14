"""Usage-metering ledger — the canonical cost/usage write+read path (Slice 4).

The append-only ``usage_events`` table (migrations/audit/0002) is the system of
record for metered cost: LLM tokens (materialized from the audit trail) and
workspace compute (emitted by the orchestrator at open/close). This module owns
the **write** path (idempotent, rate-snapshotting)
and the raw aggregate **read** that ``/api/usage`` serves; the :class:`AuditStore`
reader stays focused on the agent trace.

Design: ``knowledge-base/knowledge/features/observability_and_quotas.md`` ("The spine: one usage
ledger" + "The rate table"). Two pieces:

- :class:`UsageRates` — effective-dated $/unit resolver over the app-DB
  ``usage_rates`` table. LLM rates (prompt/cached-prompt/completion-token) are
  auto-seeded from OpenRouter's catalog by
  ``openrouter_pricing.llm_pricing_sync_loop``; compute rates (vcpu/gib-hour) are
  still unseeded. An unpriced (category, resource, unit) resolves to ``None`` →
  the quantity is metered immediately and ``cost_usd`` stays NULL until a rate
  exists.
- :class:`UsageLedger` — bulk idempotent INSERT into ``usage_events`` (ON CONFLICT
  on the at-least-once dedupe key) with the rate snapshotted onto each row, plus
  the visibility-scoped aggregate read.

Non-load-bearing, like the rest of the audit tier: when the audit pool is absent
(``AUDIT_POSTGRES_*`` unset / connect failed) writes no-op and reads return empty
— metering disabled, product flow unaffected.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence

import asyncpg

logger = logging.getLogger(__name__)

PROMPT_TOKEN_UNIT = "prompt-token"
CACHED_PROMPT_TOKEN_UNIT = "cached-prompt-token"
COMPLETION_TOKEN_UNIT = "completion-token"
LLM_TOKEN_UNITS = (
    PROMPT_TOKEN_UNIT,
    CACHED_PROMPT_TOKEN_UNIT,
    COMPLETION_TOKEN_UNIT,
)

# Frozen compatibility scope for the numeric v1 endpoints/cards. Preserve every
# pre-v2 point-event category (LLM, TTS, STT) plus the original workspace
# CPU/RAM tuple. New typed VM, agent, platform, claim, and volume rows must not
# leak into category/unit-only aggregates where their resource class/basis would
# be lost. Keep this fragment column-unqualified for raw and daily tables.
V1_USAGE_COMPAT_PREDICATE = (
    "(category IN ('llm', 'tts', 'stt') OR "
    "(category = 'compute' AND resource = 'workspace_pod' "
    "AND unit IN ('vcpu-hour', 'gib-hour')))"
)


# Distinguishes "memoized as None" from "never looked up" in the caches below.
_UNSET = object()


def _dec(x: Any) -> Optional[Decimal]:
    """Coerce to Decimal for asyncpg's ``numeric`` codec (None stays None)."""
    return None if x is None else Decimal(str(x))


def _uuid(x: Any) -> Optional[uuid.UUID]:
    """Coerce to uuid.UUID (None stays None); raises on a malformed id."""
    if x is None:
        return None
    return x if isinstance(x, uuid.UUID) else uuid.UUID(str(x))


def cache_hit_ratio_from_rows(rows: Sequence[Dict[str, Any]]) -> float:
    """Return cached prompt / total prompt for usage aggregate rows."""
    uncached = 0.0
    cached = 0.0
    for r in rows:
        if r.get("category") not in (None, "llm"):
            continue
        qty = float(r.get("quantity") or 0.0)
        if r.get("unit") == PROMPT_TOKEN_UNIT:
            uncached += qty
        elif r.get("unit") == CACHED_PROMPT_TOKEN_UNIT:
            cached += qty
    total_prompt = uncached + cached
    return cached / total_prompt if total_prompt > 0 else 0.0


def llm_tokens_from_rows(rows: Sequence[Dict[str, Any]]) -> float:
    """Return the summed LLM token quantity for usage aggregate rows."""
    return sum(
        float(r.get("quantity") or 0.0)
        for r in rows
        if r.get("unit") in LLM_TOKEN_UNITS
    )


@dataclass
class UsageEvent:
    """One metered resource *dimension* (a vcpu-hour line, a prompt-token line).

    ``rate_usd`` / ``cost_usd`` are left None by emitters; :meth:`UsageLedger.
    record_events` snapshots the rate at write time. ``ts`` is the usage / interval
    -end time (emitter-supplied so the dedupe key is deterministic).
    """

    category: str  # 'llm' | 'compute' | 'query' | 'storage'
    resource: str  # '<model_id>' | 'workspace_pod' | ...
    quantity: Any  # int/float/Decimal in `unit`
    unit: str  # 'prompt-token' | 'vcpu-hour' | ...
    source: str  # e.g. 'audit' | 'orchestrator' (idempotency namespace)
    source_id: str  # request_id / deterministic interval key
    ts: datetime
    user_id: Optional[str] = None
    project_id: Optional[str] = None
    ref_kind: Optional[str] = None  # 'job' | 'thread'
    ref_id: Optional[str] = None
    rate_usd: Any = None
    cost_usd: Any = None
    details: Dict[str, Any] = field(default_factory=dict)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STRICT_USAGE_EVENT_FIELDS = (
    "ts",
    "user_id",
    "project_id",
    "ref_kind",
    "ref_id",
    "category",
    "resource",
    "quantity",
    "unit",
    "rate_usd",
    "cost_usd",
    "source",
    "source_id",
    "details",
    "period_start",
    "period_end",
    "measurement_basis",
    "cost_domain",
    "resource_class",
    "attribution_scope",
    "measurement_algorithm",
    "source_capacity_value",
    "source_capacity_unit",
    "source_cluster",
    "source_kind",
    "source_uid",
    "source_lifecycle_id",
    "source_interval_id",
    "event_kind",
    "corrects_source",
    "corrects_source_id",
    "corrects_unit",
    "corrects_ts",
    "correction_group_id",
    "correction_reason",
    "correction_actor_id",
    "discovered_at",
    "payload_hash",
)


class StrictUsageLedgerError(RuntimeError):
    """A frozen infrastructure batch was not durably verified."""


class StrictUsagePartitionMissing(StrictUsageLedgerError):
    """One or more target monthly audit partitions are not attached."""

    def __init__(self, partitions: Sequence[str]):
        self.partitions = tuple(sorted(set(partitions)))
        super().__init__(
            "missing attached audit partition(s): " + ", ".join(self.partitions)
        )


class StrictUsageConflict(StrictUsageLedgerError):
    """A deterministic audit key exists with a different frozen payload."""


@dataclass(frozen=True)
class StrictUsageEvent:
    """One already-rated and already-hashed v2 infrastructure audit row.

    Unlike :class:`UsageEvent`, this contract never resolves a mutable rate and
    never falls back to best-effort per-row inserts.  The payload is the exact
    frozen JSON object held by the app-side publication plan.
    """

    payload: Mapping[str, Any]
    row_hash: str

    def __post_init__(self) -> None:
        payload = dict(self.payload)
        missing = set(_STRICT_USAGE_EVENT_FIELDS) - payload.keys()
        extra = payload.keys() - set(_STRICT_USAGE_EVENT_FIELDS)
        if missing or extra:
            problems: list[str] = []
            if missing:
                problems.append("missing " + ", ".join(sorted(missing)))
            if extra:
                problems.append("unknown " + ", ".join(sorted(extra)))
            raise ValueError("invalid frozen usage payload: " + "; ".join(problems))
        if not _SHA256_RE.fullmatch(self.row_hash):
            raise ValueError("frozen usage row_hash must be lowercase SHA-256")
        if payload["payload_hash"] != self.row_hash:
            raise ValueError("frozen usage payload_hash does not match row_hash")
        if payload["source"] not in {
            "infra-allocation-v2",
            "infra-allocation-correction-v2",
        }:
            raise ValueError("strict usage event must use an infrastructure source")
        if not payload["source_id"] or not payload["unit"]:
            raise ValueError("strict usage event requires source_id and unit")
        timestamp = self.timestamp
        if (
            timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")
            != payload["ts"]
        ):
            raise ValueError("strict usage event ts must be canonical UTC microseconds")
        object.__setattr__(self, "payload", payload)

    @property
    def timestamp(self) -> datetime:
        raw = self.payload.get("ts")
        if not isinstance(raw, str):
            raise ValueError("strict usage event ts must be text")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("strict usage event ts is invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("strict usage event ts must be timezone-aware")
        return parsed.astimezone(timezone.utc)

    @property
    def dedupe_key(self) -> tuple[str, str, str, str]:
        return (
            str(self.payload["source"]),
            str(self.payload["source_id"]),
            str(self.payload["unit"]),
            str(self.payload["ts"]),
        )


@dataclass(frozen=True)
class StrictUsageExpectation:
    """Read-only verification contract for one immutable ledger row.

    The full dedupe key is mandatory. ``expected_fields`` may contain any
    allowlisted immutable ``usage_events`` column, enabling the cutover drain
    to verify legacy rows that predate ``payload_hash`` while typed callers can
    demand the complete frozen payload.
    """

    source: str
    source_id: str
    unit: str
    ts: datetime
    expected_fields: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.source or not self.source_id or not self.unit:
            raise ValueError("strict usage expectation requires a full dedupe key")
        timestamp = self.ts
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("strict usage expectation ts must be timezone-aware")
        timestamp = timestamp.astimezone(timezone.utc)
        fields = dict(self.expected_fields)
        if not fields:
            raise ValueError("strict usage expectation requires immutable fields")
        unknown = fields.keys() - set(_STRICT_USAGE_EVENT_FIELDS)
        if unknown:
            raise ValueError(
                "strict usage expectation has unknown field(s): "
                + ", ".join(sorted(unknown))
            )
        canonical_ts = timestamp.isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )
        key_values = {
            "source": self.source,
            "source_id": self.source_id,
            "unit": self.unit,
            "ts": canonical_ts,
        }
        for name, expected in key_values.items():
            if (
                name in fields
                and _normalize_verified_value(name, fields[name]) != expected
            ):
                raise ValueError(
                    f"strict usage expectation {name} differs from its dedupe key"
                )
        object.__setattr__(self, "ts", timestamp)
        object.__setattr__(self, "expected_fields", fields)

    @property
    def dedupe_key(self) -> tuple[str, str, str, str]:
        return (
            self.source,
            self.source_id,
            self.unit,
            self.ts.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        )


@dataclass(frozen=True)
class StrictUsagePublishResult:
    expected: int
    inserted: int
    verified: int


class UsageRates:
    """Effective-dated rate resolver over the app-DB ``usage_rates`` table.

    The table is small config → cache all rows in memory with a short TTL and
    resolve the newest ``effective_from <= ts`` in Python (specific resource
    first, then the ``'*'`` category default). LLM rates are auto-seeded from
    OpenRouter (``openrouter_pricing``); compute rates are not yet seeded, so an
    unpriced (category, resource, unit) resolves to ``None`` and the event's
    quantity is metered with cost NULL.
    """

    def __init__(self, app_pool: Optional[asyncpg.Pool], *, ttl_s: float = 300.0):
        self._pool = app_pool
        self._ttl = ttl_s
        self._rows: List[Dict[str, Any]] = []
        self._loaded_at: float = -1.0

    async def _ensure_loaded(self) -> None:
        if self._pool is None:
            return
        now = time.monotonic()
        if self._loaded_at >= 0 and (now - self._loaded_at) < self._ttl:
            return
        try:
            rows = await self._pool.fetch(
                "SELECT category, resource, unit, rate_usd, effective_from "
                "FROM usage_rates"
            )
            self._rows = [dict(r) for r in rows]
            self._loaded_at = now
        except Exception:
            # Missing table / transient error → treat as unpriced, retry next call.
            logger.warning(
                "usage_rates load failed; treating as unpriced", exc_info=True
            )

    async def resolve(
        self, category: str, resource: str, unit: str, ts: datetime
    ) -> Optional[Decimal]:
        await self._ensure_loaded()
        if not self._rows:
            return None
        for want in (resource, "*"):
            cands = [
                r
                for r in self._rows
                if r["category"] == category
                and r["resource"] == want
                and r["unit"] == unit
                and r["effective_from"] <= ts
            ]
            if cands:
                return max(cands, key=lambda r: r["effective_from"])["rate_usd"]
        return None


# Dimension → the usage_events column it groups on. The allow-list is also the
# validation guard: an unknown group_by raises rather than interpolating SQL.
_GROUP_COLS = {"user": "user_id", "model": "resource", "project": "project_id"}

# Parallel-unnest bulk insert. details rides as text[] and casts to jsonb in SQL
# (codec-independent — no dependency on the pool having a jsonb codec). ON CONFLICT
# on the dedupe index makes a re-polled spend log / re-emitted close a no-op.
_INSERT_SQL = """
INSERT INTO usage_events
    (ts, user_id, project_id, ref_kind, ref_id, category, resource,
     quantity, unit, rate_usd, cost_usd, source, source_id, details)
SELECT ts, user_id, project_id, ref_kind, ref_id, category, resource,
       quantity, unit, rate_usd, cost_usd, source, source_id, details::jsonb
FROM unnest(
    $1::timestamptz[], $2::uuid[], $3::uuid[], $4::text[], $5::uuid[],
    $6::text[], $7::text[], $8::numeric[], $9::text[], $10::numeric[],
    $11::numeric[], $12::text[], $13::text[], $14::text[]
) AS t(ts, user_id, project_id, ref_kind, ref_id, category, resource,
       quantity, unit, rate_usd, cost_usd, source, source_id, details)
ON CONFLICT (source, source_id, unit, ts) DO NOTHING
"""

_LEGACY_FROZEN_EVENT_FIELDS = frozenset(
    {
        "ts",
        "user_id",
        "project_id",
        "ref_kind",
        "ref_id",
        "category",
        "resource",
        "quantity",
        "unit",
        "rate_usd",
        "cost_usd",
        "source",
        "source_id",
        "details",
    }
)

_STRICT_LEGACY_INSERT_SQL = """
/* strict-usage:insert-expected */
INSERT INTO usage_events (
    ts, user_id, project_id, ref_kind, ref_id, category, resource,
    quantity, unit, rate_usd, cost_usd, source, source_id, details
)
SELECT
    event.ts, event.user_id, event.project_id, event.ref_kind, event.ref_id,
    event.category, event.resource, event.quantity, event.unit,
    event.rate_usd, event.cost_usd, event.source, event.source_id,
    event.details
FROM jsonb_to_recordset($1::jsonb) AS event(
    ts timestamptz, user_id uuid, project_id uuid, ref_kind text, ref_id uuid,
    category text, resource text, quantity numeric, unit text,
    rate_usd numeric, cost_usd numeric, source text, source_id text,
    details jsonb
)
ON CONFLICT (source, source_id, unit, ts) DO NOTHING
"""

_STRICT_ATTACHED_PARTITIONS_SQL = """
/* strict-usage:attached-partitions */
SELECT child.relname
FROM pg_inherits inheritance
JOIN pg_class child ON child.oid = inheritance.inhrelid
WHERE inheritance.inhparent = 'public.usage_events'::regclass
  AND child.relname = ANY($1::text[])
  AND NOT inheritance.inhdetachpending
"""

_STRICT_INSERT_SQL = """
/* strict-usage:insert-frozen */
INSERT INTO usage_events (
    ts, user_id, project_id, ref_kind, ref_id, category, resource,
    quantity, unit, rate_usd, cost_usd, source, source_id, details,
    period_start, period_end, measurement_basis, cost_domain,
    resource_class, attribution_scope, measurement_algorithm,
    source_capacity_value, source_capacity_unit, source_cluster,
    source_kind, source_uid, source_lifecycle_id, source_interval_id,
    event_kind, corrects_source, corrects_source_id, corrects_unit,
    corrects_ts, correction_group_id, correction_reason,
    correction_actor_id, discovered_at, payload_hash
)
SELECT
    event.ts, event.user_id, event.project_id, event.ref_kind, event.ref_id,
    event.category, event.resource, event.quantity, event.unit,
    event.rate_usd, event.cost_usd, event.source, event.source_id,
    event.details, event.period_start, event.period_end,
    event.measurement_basis, event.cost_domain, event.resource_class,
    event.attribution_scope, event.measurement_algorithm,
    event.source_capacity_value, event.source_capacity_unit,
    event.source_cluster, event.source_kind, event.source_uid,
    event.source_lifecycle_id, event.source_interval_id, event.event_kind,
    event.corrects_source, event.corrects_source_id, event.corrects_unit,
    event.corrects_ts, event.correction_group_id, event.correction_reason,
    event.correction_actor_id, event.discovered_at, event.payload_hash
FROM jsonb_to_recordset($1::jsonb) AS event(
    ts timestamptz, user_id uuid, project_id uuid, ref_kind text, ref_id uuid,
    category text, resource text, quantity numeric, unit text,
    rate_usd numeric, cost_usd numeric, source text, source_id text,
    details jsonb, period_start timestamptz, period_end timestamptz,
    measurement_basis text, cost_domain text, resource_class text,
    attribution_scope text, measurement_algorithm text,
    source_capacity_value numeric, source_capacity_unit text,
    source_cluster text, source_kind text, source_uid text,
    source_lifecycle_id uuid, source_interval_id uuid, event_kind text,
    corrects_source text, corrects_source_id text, corrects_unit text,
    corrects_ts timestamptz, correction_group_id uuid,
    correction_reason text, correction_actor_id uuid,
    discovered_at timestamptz, payload_hash text
)
ON CONFLICT (source, source_id, unit, ts) DO NOTHING
"""

_STRICT_VERIFY_SQL = """
/* strict-usage:verify-frozen */
WITH expected AS (
    SELECT *
    FROM jsonb_to_recordset($1::jsonb) AS item(
        source text, source_id text, unit text, ts timestamptz,
        payload_hash text
    )
)
SELECT
    expected.source,
    expected.source_id,
    expected.unit,
    to_char(expected.ts AT TIME ZONE 'UTC',
            'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS ts,
    expected.payload_hash AS expected_hash,
    actual.payload_hash AS actual_hash
FROM expected
LEFT JOIN usage_events actual
  ON actual.source = expected.source
 AND actual.source_id = expected.source_id
 AND actual.unit = expected.unit
 AND actual.ts = expected.ts
ORDER BY expected.source, expected.source_id, expected.unit, expected.ts
"""

_STRICT_EXPECTATION_VERIFY_SQL = """
/* strict-usage:verify-expected */
WITH expected AS (
    SELECT *
    FROM jsonb_to_recordset($1::jsonb) AS item(
        ordinal integer, source text, source_id text, unit text,
        ts timestamptz
    )
)
SELECT
    expected.ordinal,
    expected.source,
    expected.source_id,
    expected.unit,
    to_char(expected.ts AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS ts,
    actual.id IS NOT NULL AS present,
    CASE WHEN actual.id IS NULL THEN NULL
         ELSE to_jsonb(actual) - 'id'
    END AS actual_payload
FROM expected
LEFT JOIN usage_events AS actual
  ON actual.source = expected.source
 AND actual.source_id = expected.source_id
 AND actual.unit = expected.unit
 AND actual.ts = expected.ts
ORDER BY expected.ordinal
"""

_VERIFIED_NUMERIC_FIELDS = frozenset(
    {"quantity", "rate_usd", "cost_usd", "source_capacity_value"}
)
_VERIFIED_UUID_FIELDS = frozenset(
    {
        "user_id",
        "project_id",
        "ref_id",
        "source_lifecycle_id",
        "source_interval_id",
        "correction_group_id",
        "correction_actor_id",
    }
)
_VERIFIED_TIMESTAMP_FIELDS = frozenset(
    {"ts", "period_start", "period_end", "corrects_ts", "discovered_at"}
)


def _canonical_verified_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("verified timestamp is invalid") from exc
    else:
        raise ValueError("verified timestamp must be datetime or text")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("verified timestamp must be timezone-aware")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _normalize_verified_json(value: Any) -> Any:
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, Decimal | int | float):
        try:
            number = Decimal(str(value))
        except Exception as exc:
            raise ValueError("verified JSON number is invalid") from exc
        if not number.is_finite():
            raise ValueError("verified JSON number must be finite")
        return Decimal(0) if number.is_zero() else number.normalize()
    if isinstance(value, Mapping):
        return tuple(
            (str(key), _normalize_verified_json(child))
            for key, child in sorted(value.items(), key=lambda item: str(item[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_normalize_verified_json(child) for child in value)
    raise ValueError(f"unsupported verified JSON value: {type(value).__name__}")


def _normalize_verified_value(field_name: str, value: Any) -> Any:
    if value is None:
        return None
    if field_name in _VERIFIED_NUMERIC_FIELDS:
        try:
            parsed = Decimal(str(value))
        except Exception as exc:
            raise ValueError(f"{field_name} is not an exact decimal") from exc
        if not parsed.is_finite():
            raise ValueError(f"{field_name} must be finite")
        return Decimal(0) if parsed.is_zero() else parsed.normalize()
    if field_name in _VERIFIED_UUID_FIELDS:
        return str(uuid.UUID(str(value)))
    if field_name in _VERIFIED_TIMESTAMP_FIELDS:
        return _canonical_verified_timestamp(value)
    if field_name == "details":
        if isinstance(value, str):
            try:
                value = json.loads(value, parse_float=Decimal, parse_int=Decimal)
            except json.JSONDecodeError as exc:
                raise ValueError("verified details is invalid JSON") from exc
        return _normalize_verified_json(value)
    return value


def _decoded_json_object(value: Any) -> Mapping[str, Any]:
    if isinstance(value, str):
        decoded = json.loads(value, parse_float=Decimal, parse_int=Decimal)
    else:
        decoded = value
    if not isinstance(decoded, Mapping):
        raise StrictUsageConflict("audit verification payload is not an object")
    return decoded


def _verified_wire_value(field_name: str, value: Any) -> Any:
    if field_name == "details":
        if isinstance(value, str):
            return json.loads(value)
        return value
    normalized = _normalize_verified_value(field_name, value)
    if isinstance(normalized, Decimal):
        return format(normalized, "f")
    return normalized


def _verify_expectation_rows(
    expectations: Sequence[StrictUsageExpectation],
    rows: Sequence[Mapping[str, Any]],
) -> int:
    if len(rows) != len(expectations):
        raise StrictUsageConflict(
            "audit verification returned an incomplete expectation set"
        )
    conflicts: list[str] = []
    verified = 0
    for ordinal, expectation in enumerate(expectations):
        row = rows[ordinal]
        if int(row["ordinal"]) != ordinal or not bool(row["present"]):
            conflicts.append("/".join(expectation.dedupe_key) + ":missing")
            continue
        actual = _decoded_json_object(row["actual_payload"])
        mismatched = []
        for field_name, expected_value in expectation.expected_fields.items():
            if field_name not in actual or _normalize_verified_value(
                field_name, actual.get(field_name)
            ) != _normalize_verified_value(field_name, expected_value):
                mismatched.append(field_name)
        if mismatched:
            conflicts.append(
                "/".join(expectation.dedupe_key) + ":" + ",".join(sorted(mismatched))
            )
        else:
            verified += 1
    if conflicts:
        raise StrictUsageConflict(
            "audit row missing or immutable field mismatch: " + "; ".join(conflicts[:3])
        )
    return verified


class UsageLedger:
    """Write + raw-read surface for the ``usage_events`` ledger (auditdb)."""

    def __init__(self, audit_pool: Optional[asyncpg.Pool], rates: UsageRates):
        self._pool = audit_pool
        self._rates = rates
        # source -> first ts ever recorded; see earliest_event_ts. Only non-NULL
        # results land here, so an empty ledger keeps re-asking.
        self._earliest_ts_cache: Dict[str, datetime] = {}

    @property
    def is_available(self) -> bool:
        return self._pool is not None

    async def verify_frozen_events(
        self, events: Sequence[StrictUsageEvent]
    ) -> StrictUsagePublishResult:
        """Verify complete typed payloads without inserting or repairing rows."""

        return await self.verify_expected_events(
            [
                StrictUsageExpectation(
                    source=str(event.payload["source"]),
                    source_id=str(event.payload["source_id"]),
                    unit=str(event.payload["unit"]),
                    ts=event.timestamp,
                    expected_fields=dict(event.payload),
                )
                for event in events
            ]
        )

    async def verify_expected_events(
        self, expectations: Sequence[StrictUsageExpectation]
    ) -> StrictUsagePublishResult:
        """Strictly verify existing rows by full key and immutable fields.

        This read-only path is shared by typed correction planning and the
        legacy cutover drain.  It never performs an insert, rate lookup, or
        best-effort fallback.
        """

        if self._pool is None:
            raise StrictUsageLedgerError("audit usage ledger is unavailable")
        if not expectations:
            raise ValueError("strict usage verification requires expectations")
        keys = [expectation.dedupe_key for expectation in expectations]
        if len(set(keys)) != len(keys):
            raise ValueError("strict usage verification contains duplicate keys")

        partitions = {
            f"usage_events_p{item.ts.year:04d}_{item.ts.month:02d}"
            for item in expectations
        }
        key_payload = json.dumps(
            [
                {
                    "ordinal": ordinal,
                    "source": item.source,
                    "source_id": item.source_id,
                    "unit": item.unit,
                    "ts": item.dedupe_key[3],
                }
                for ordinal, item in enumerate(expectations)
            ],
            separators=(",", ":"),
            sort_keys=True,
        )

        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                attached_rows = await conn.fetch(
                    _STRICT_ATTACHED_PARTITIONS_SQL, sorted(partitions)
                )
                attached = {str(row["relname"]) for row in attached_rows}
                missing = sorted(partitions - attached)
                if missing:
                    raise StrictUsagePartitionMissing(missing)
                rows = await conn.fetch(_STRICT_EXPECTATION_VERIFY_SQL, key_payload)

        verified = _verify_expectation_rows(expectations, rows)
        return StrictUsagePublishResult(
            expected=len(expectations),
            inserted=0,
            verified=verified,
        )

    async def publish_expected_events(
        self, expectations: Sequence[StrictUsageExpectation]
    ) -> StrictUsagePublishResult:
        """Atomically repair frozen legacy 14-column rows and verify them.

        Unlike :meth:`record_events`, this cutover-only boundary accepts
        already-rated payloads, performs no mutable rate lookup, and rolls the
        complete batch back if any pre-existing dedupe key differs.
        """

        if self._pool is None:
            raise StrictUsageLedgerError("audit usage ledger is unavailable")
        if not expectations:
            raise ValueError("strict legacy publication requires expectations")
        keys = [expectation.dedupe_key for expectation in expectations]
        if len(set(keys)) != len(keys):
            raise ValueError("strict legacy publication contains duplicate keys")
        for expectation in expectations:
            fields = set(expectation.expected_fields)
            if fields != _LEGACY_FROZEN_EVENT_FIELDS:
                missing = sorted(_LEGACY_FROZEN_EVENT_FIELDS - fields)
                extra = sorted(fields - _LEGACY_FROZEN_EVENT_FIELDS)
                problems = []
                if missing:
                    problems.append("missing " + ", ".join(missing))
                if extra:
                    problems.append("unknown " + ", ".join(extra))
                raise ValueError(
                    "strict legacy payload must freeze all 14 columns: "
                    + "; ".join(problems)
                )

        partitions = {
            f"usage_events_p{item.ts.year:04d}_{item.ts.month:02d}"
            for item in expectations
        }
        payload_rows = [
            {
                field_name: _verified_wire_value(
                    field_name,
                    expectation.expected_fields[field_name],
                )
                for field_name in _LEGACY_FROZEN_EVENT_FIELDS
            }
            for expectation in expectations
        ]
        frozen_payload = json.dumps(
            payload_rows,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        key_payload = json.dumps(
            [
                {
                    "ordinal": ordinal,
                    "source": item.source,
                    "source_id": item.source_id,
                    "unit": item.unit,
                    "ts": item.dedupe_key[3],
                }
                for ordinal, item in enumerate(expectations)
            ],
            separators=(",", ":"),
            sort_keys=True,
        )

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                attached_rows = await conn.fetch(
                    _STRICT_ATTACHED_PARTITIONS_SQL, sorted(partitions)
                )
                attached = {str(row["relname"]) for row in attached_rows}
                missing = sorted(partitions - attached)
                if missing:
                    raise StrictUsagePartitionMissing(missing)
                status = await conn.execute(_STRICT_LEGACY_INSERT_SQL, frozen_payload)
                inserted = int(str(status).split()[-1])
                rows = await conn.fetch(_STRICT_EXPECTATION_VERIFY_SQL, key_payload)
                verified = _verify_expectation_rows(expectations, rows)

        return StrictUsagePublishResult(
            expected=len(expectations),
            inserted=inserted,
            verified=verified,
        )

    async def publish_frozen_events(
        self, events: Sequence[StrictUsageEvent]
    ) -> StrictUsagePublishResult:
        """Atomically insert and verify a frozen infrastructure batch.

        This is deliberately separate from :meth:`record_events`: it never
        resolves rates, never falls back to per-row writes, and never swallows
        an exception.  Replays are successful only when every deterministic
        audit key already carries the exact frozen payload hash.
        """
        if self._pool is None:
            raise StrictUsageLedgerError("audit usage ledger is unavailable")
        if not events:
            raise ValueError("strict usage publication requires at least one event")

        keys = [event.dedupe_key for event in events]
        if len(set(keys)) != len(keys):
            raise ValueError("strict usage publication contains duplicate keys")

        partitions = {
            f"usage_events_p{event.timestamp.year:04d}_{event.timestamp.month:02d}"
            for event in events
        }
        frozen_payload = json.dumps(
            [dict(event.payload) for event in events],
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        expected_payload = json.dumps(
            [
                {
                    "source": event.payload["source"],
                    "source_id": event.payload["source_id"],
                    "unit": event.payload["unit"],
                    "ts": event.payload["ts"],
                    "payload_hash": event.row_hash,
                }
                for event in events
            ],
            separators=(",", ":"),
            sort_keys=True,
        )

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                attached_rows = await conn.fetch(
                    _STRICT_ATTACHED_PARTITIONS_SQL, sorted(partitions)
                )
                attached = {str(row["relname"]) for row in attached_rows}
                missing = sorted(partitions - attached)
                if missing:
                    raise StrictUsagePartitionMissing(missing)

                status = await conn.execute(_STRICT_INSERT_SQL, frozen_payload)
                inserted = int(str(status).split()[-1])
                verified_rows = await conn.fetch(_STRICT_VERIFY_SQL, expected_payload)
                if len(verified_rows) != len(events):
                    raise StrictUsageConflict(
                        "audit verification returned an incomplete key set"
                    )
                conflicts = [
                    (
                        str(row["source"]),
                        str(row["source_id"]),
                        str(row["unit"]),
                        str(row["ts"]),
                    )
                    for row in verified_rows
                    if row["actual_hash"] is None
                    or row["actual_hash"] != row["expected_hash"]
                ]
                if conflicts:
                    rendered = ", ".join("/".join(key) for key in conflicts[:3])
                    raise StrictUsageConflict(
                        f"audit row missing or payload hash mismatch: {rendered}"
                    )

        return StrictUsagePublishResult(
            expected=len(events), inserted=inserted, verified=len(verified_rows)
        )

    async def resolve_rate(
        self, category: str, resource: str, unit: str, ts: datetime
    ) -> Optional[Decimal]:
        """The configured rate an event at ``ts`` would be snapshotted with."""
        return await self._rates.resolve(category, resource, unit, ts)

    async def record_events(self, events: Sequence[UsageEvent]) -> int:
        """Idempotently insert a batch; returns the count *actually* inserted.

        Snapshots the rate onto any event that doesn't already carry one (the
        emitter usually leaves rate/cost None). Re-emitted rows collide on the
        dedupe key and are skipped (counted out of the return value), so the
        at-least-once emitters can safely retry.
        """
        if self._pool is None or not events:
            return 0

        for e in events:
            if e.rate_usd is None and e.cost_usd is None:
                rate = await self._rates.resolve(e.category, e.resource, e.unit, e.ts)
                if rate is not None:
                    e.rate_usd = rate
                    e.cost_usd = Decimal(str(e.quantity)) * rate

        cols: Dict[str, list] = {
            "ts": [e.ts for e in events],
            "user_id": [_uuid(e.user_id) for e in events],
            "project_id": [_uuid(e.project_id) for e in events],
            "ref_kind": [e.ref_kind for e in events],
            "ref_id": [_uuid(e.ref_id) for e in events],
            "category": [e.category for e in events],
            "resource": [e.resource for e in events],
            "quantity": [_dec(e.quantity) for e in events],
            "unit": [e.unit for e in events],
            "rate_usd": [_dec(e.rate_usd) for e in events],
            "cost_usd": [_dec(e.cost_usd) for e in events],
            "source": [e.source for e in events],
            "source_id": [e.source_id for e in events],
            "details": [json.dumps(e.details or {}) for e in events],
        }
        try:
            async with self._pool.acquire() as conn:
                status = await conn.execute(_INSERT_SQL, *cols.values())
            # status is "INSERT 0 <n>" where n excludes ON CONFLICT skips.
            inserted = int(status.split()[-1])
            if inserted:
                logger.debug("usage ledger: +%d event(s)", inserted)
            return inserted
        except Exception:
            # One bad row (e.g. a ts outside any partition) must not sink the whole
            # batch — and since the poller re-sees the same rows, a sunk batch would
            # block EVERY future materialization. Fall back to per-row inserts so the
            # good rows land and only the offender is dropped.
            logger.warning(
                "usage ledger batch insert failed; retrying per-row", exc_info=True
            )
            return await self._insert_per_row(cols, events)

    async def _insert_per_row(
        self, cols: Dict[str, list], events: Sequence["UsageEvent"]
    ) -> int:
        """Fallback: insert each event alone so one uninsertable row is isolated."""
        inserted = 0
        for i in range(len(events)):
            one = {k: [v[i]] for k, v in cols.items()}
            try:
                async with self._pool.acquire() as conn:
                    status = await conn.execute(_INSERT_SQL, *one.values())
                inserted += int(status.split()[-1])
            except Exception:
                e = events[i]
                logger.warning(
                    "usage ledger: dropping uninsertable event source=%s "
                    "source_id=%s unit=%s (non-fatal)",
                    e.source,
                    e.source_id,
                    e.unit,
                    exc_info=True,
                )
        return inserted

    async def query_usage(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
        ref_id: Optional[str] = None,
        ref_ids: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Aggregate usage in [from_ts, to_ts), grouped by (category, unit).

        Visibility (G5) mirrors the stats endpoints: an ``owner_user_id`` (a
        non-admin caller) restricts to rows they own or a project they can see;
        an admin passes neither (full fleet) or just ``scope_project_id``. Summing
        quantity is only meaningful within a unit, hence the (category, unit)
        grouping; ``cost_usd`` is summed across all rows for the headline total.
        """
        empty = {"by_category": [], "total_cost_usd": 0.0, "cache_hit_ratio": 0.0}
        if self._pool is None:
            return empty

        clauses = ["ts >= $1", "ts < $2", V1_USAGE_COMPAT_PREDICATE]
        params: List[Any] = [from_ts, to_ts]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            own = f"user_id = ${len(params)}"
            pids = [_uuid(p) for p in (visible_project_ids or [])]
            if pids:
                params.append(pids)
                clauses.append(f"({own} OR project_id = ANY(${len(params)}::uuid[]))")
            else:
                clauses.append(own)
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        if ref_id is not None:
            params.append(_uuid(ref_id))
            clauses.append(f"ref_id = ${len(params)}")
        if ref_ids is not None:
            # Set form of ref_id, for costing a GROUP of jobs in one round trip
            # — the officer's per-slot spend ceiling sums today's cost across
            # every job stamped with that slot, and the job set comes from the
            # app DB while the events live here, so a join is not available.
            # An empty set means "no jobs", which must match no rows rather
            # than degrade to the whole project.
            params.append([_uuid(r) for r in ref_ids])
            clauses.append(f"ref_id = ANY(${len(params)}::uuid[])")

        sql = (
            "SELECT category, unit, SUM(quantity) AS quantity, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, COUNT(*) AS events "
            f"FROM usage_events WHERE {' AND '.join(clauses)} "
            "GROUP BY category, unit ORDER BY category, unit"
        )
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except Exception:
            logger.warning("usage ledger query failed (non-fatal)", exc_info=True)
            return empty

        by_category = [
            {
                "category": r["category"],
                "unit": r["unit"],
                "quantity": float(r["quantity"]) if r["quantity"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": r["events"],
            }
            for r in rows
        ]
        total = sum(item["cost_usd"] for item in by_category)
        return {
            "by_category": by_category,
            "total_cost_usd": total,
            "cache_hit_ratio": cache_hit_ratio_from_rows(by_category),
        }

    async def query_grouped(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        group_by: str,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate quantity/cost/events grouped by (dimension, unit).

        ``group_by`` ∈ {'user','model','project'} → user_id / resource / project_id.
        Rows with a NULL group key (unattributed fleet-key LLM traffic) are excluded
        so they don't collapse into one bogus bucket. Visibility differs from
        ``query_usage`` on purpose: a **non-admin** (``owner_user_id`` set) is scoped
        strictly to their OWN rows — a breakdown must not disclose other users who
        merely share a visible project. Admins pass no owner (full fleet) or a
        ``scope_project_id``.
        """
        if group_by not in _GROUP_COLS:
            raise ValueError(f"unsupported group_by: {group_by!r}")
        col = _GROUP_COLS[group_by]
        if self._pool is None:
            return []
        clauses = [
            "ts >= $1",
            "ts < $2",
            f"{col} IS NOT NULL",
            V1_USAGE_COMPAT_PREDICATE,
        ]
        if group_by == "model":
            clauses.append("category = 'llm'")
        params: List[Any] = [from_ts, to_ts]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            clauses.append(f"user_id = ${len(params)}")  # strict self-scope
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        sql = (
            f"SELECT {col} AS key, unit, SUM(quantity) AS quantity, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, COUNT(*) AS events "
            f"FROM usage_events WHERE {' AND '.join(clauses)} "
            f"GROUP BY {col}, unit ORDER BY {col}"
        )
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except Exception:
            logger.warning("usage grouped query failed (non-fatal)", exc_info=True)
            return []
        return [
            {
                "key": str(r["key"]),
                "unit": r["unit"],
                "quantity": float(r["quantity"]) if r["quantity"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": r["events"],
            }
            for r in rows
        ]

    async def query_timeseries(
        self,
        *,
        from_ts: datetime,
        to_ts: datetime,
        group_by: str,
        owner_user_id: Optional[str] = None,
        visible_project_ids: Optional[Sequence[str]] = None,
        scope_project_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Daily-bucketed usage grouped by (UTC day, dimension).

        Like ``query_grouped`` but adds a ``date_trunc('day', ts)`` bucket so the
        caller can draw a stacked usage-over-time chart. Each row carries the three
        dimension-agnostic metrics — ``tokens`` (prompt+cached+completion summed),
        the priced ``cost_usd``, and ``events`` — for one (day, key). The day is
        truncated in UTC (``ts AT TIME ZONE 'UTC'``) so buckets don't drift with the
        DB session timezone. NULL group keys are excluded and visibility (G5) is
        identical to ``query_grouped`` — a non-admin is scoped strictly to self.
        """
        if group_by not in _GROUP_COLS:
            raise ValueError(f"unsupported group_by: {group_by!r}")
        col = _GROUP_COLS[group_by]
        if self._pool is None:
            return []
        clauses = [
            "ts >= $1",
            "ts < $2",
            f"{col} IS NOT NULL",
            V1_USAGE_COMPAT_PREDICATE,
        ]
        if group_by == "model":
            clauses.append("category = 'llm'")
        params: List[Any] = [from_ts, to_ts]
        if owner_user_id is not None:
            params.append(_uuid(owner_user_id))
            clauses.append(f"user_id = ${len(params)}")  # strict self-scope
        if scope_project_id is not None:
            params.append(_uuid(scope_project_id))
            clauses.append(f"project_id = ${len(params)}")
        sql = (
            "SELECT date_trunc('day', ts AT TIME ZONE 'UTC') AS day, "
            f"{col} AS key, "
            "COALESCE(SUM(quantity) FILTER "
            "(WHERE unit IN "
            "('prompt-token', 'cached-prompt-token', 'completion-token')), 0) "
            "AS tokens, "
            "COALESCE(SUM(cost_usd), 0) AS cost_usd, COUNT(*) AS events "
            f"FROM usage_events WHERE {' AND '.join(clauses)} "
            f"GROUP BY date_trunc('day', ts AT TIME ZONE 'UTC'), {col} "
            "ORDER BY date_trunc('day', ts AT TIME ZONE 'UTC')"
        )
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except Exception:
            logger.warning("usage timeseries query failed (non-fatal)", exc_info=True)
            return []
        return [
            {
                "day": r["day"].date().isoformat(),
                "key": str(r["key"]),
                "tokens": float(r["tokens"]) if r["tokens"] is not None else 0.0,
                "cost_usd": float(r["cost_usd"]),
                "events": r["events"],
            }
            for r in rows
        ]

    async def query_ref_usage(
        self,
        *,
        ref_kind: str,
        ref_ids: Sequence[str],
        from_ts: datetime,
        to_ts: datetime,
    ) -> List[Dict[str, Any]]:
        """Per-(category, resource, unit) usage for one job/thread, or a subtree.

        The read behind ``GET /api/jobs/{job_id}/usage``. Three deliberate
        departures from :meth:`query_usage`, each one so that "what did this
        cost?" can be answered without lying:

        * **No** ``V1_USAGE_COMPAT_PREDICATE``. That fragment freezes the v1
          cards to LLM/TTS/STT plus the original workspace CPU/RAM tuple, but the
          infrastructure materializer stamps ``ref_kind`` in {job, thread} on
          typed VM / volume / agent rows too (``infrastructure_metering/
          materializer.py`` — customer-attributed intervals). Applying the compat
          scope here would silently drop a VM-backed job's machine cost, which is
          the exact failure this endpoint exists to prevent. Grouping by
          ``resource`` keeps anything new visible rather than folded away.
        * ``SUM(cost_usd)`` is **not** ``COALESCE``-d to zero. NULL means *not
          priced* (no rate card for that model or resource) and unpriced is not
          free; ``priced_events`` vs ``events`` is what lets a caller tell a fully
          priced group from a partially priced one. ``/api/usage`` collapses both
          into 0.0 and therefore reports unpriced compute as costing nothing.
        * Grouped by ``resource``, so the per-model split falls out for free.

        ``ref_ids`` is required, and an empty set matches no rows rather than
        degrading to "everything" (the officer's spend ceiling has the same rule
        for the same reason). Visibility is the caller's problem: for a job,
        ``require_job_access`` has already settled it, and a ref filter is not
        itself a G5 scope.
        """
        if self._pool is None or not ref_ids:
            return []
        sql = (
            "SELECT category, resource, unit, "
            "SUM(quantity) AS quantity, "
            "SUM(cost_usd) AS cost_usd, "
            "COUNT(*) FILTER (WHERE cost_usd IS NOT NULL) AS priced_events, "
            "COUNT(*) AS events, "
            "MIN(ts) AS first_ts, MAX(ts) AS last_ts "
            "FROM usage_events "
            "WHERE ref_kind = $1 AND ref_id = ANY($2::uuid[]) "
            "AND ts >= $3 AND ts < $4 "
            "GROUP BY category, resource, unit "
            "ORDER BY category, resource, unit"
        )
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    sql, ref_kind, [_uuid(r) for r in ref_ids], from_ts, to_ts
                )
        except Exception:
            logger.warning("usage ref query failed (non-fatal)", exc_info=True)
            return []
        return [
            {
                "category": r["category"],
                "resource": r["resource"],
                "unit": r["unit"],
                "quantity": float(r["quantity"]) if r["quantity"] is not None else 0.0,
                # Preserved as None when nothing in the group carried a rate.
                "cost_usd": None if r["cost_usd"] is None else float(r["cost_usd"]),
                "priced_events": int(r["priced_events"]),
                "events": int(r["events"]),
                "first_ts": r["first_ts"],
                "last_ts": r["last_ts"],
            }
            for r in rows
        ]

    async def earliest_event_ts(self, *, source: str = "audit") -> Optional[datetime]:
        """First ``ts`` the ledger ever recorded for ``source`` (memoized).

        The materializer's cursor is forward-only and anchored the first time it
        ran, so LLM requests older than that are never backfilled. A job created
        before this floor therefore has no usage rows *by construction* — which is
        a different answer from "this job spent nothing", and the only way the API
        can tell those two apart.

        Memoized for the process lifetime: no index leads with ``source``, so the
        honest query is a seq scan of every partition, and the value is a
        historical constant that only an operator-driven backfill moves (a
        restart-worthy act). A NULL result is *not* cached — an empty ledger is
        cheap to re-scan and will eventually have a floor.
        """
        if self._pool is None:
            return None
        cached = self._earliest_ts_cache.get(source, _UNSET)
        if cached is not _UNSET:
            return cached  # type: ignore[return-value]
        try:
            async with self._pool.acquire() as conn:
                value = await conn.fetchval(
                    "SELECT MIN(ts) FROM usage_events WHERE source = $1", source
                )
        except Exception:
            logger.warning("usage ledger floor query failed (non-fatal)", exc_info=True)
            return None
        if value is not None:
            self._earliest_ts_cache[source] = value
        return value


__all__ = [
    "StrictUsageConflict",
    "StrictUsageEvent",
    "StrictUsageExpectation",
    "StrictUsageLedgerError",
    "StrictUsagePartitionMissing",
    "StrictUsagePublishResult",
    "UsageEvent",
    "UsageRates",
    "UsageLedger",
    "PROMPT_TOKEN_UNIT",
    "CACHED_PROMPT_TOKEN_UNIT",
    "COMPLETION_TOKEN_UNIT",
    "LLM_TOKEN_UNITS",
    "V1_USAGE_COMPAT_PREDICATE",
    "cache_hit_ratio_from_rows",
    "llm_tokens_from_rows",
]
