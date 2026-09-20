"""Unconnected installation and drain state for one exact resource policy.

Drain makes the resource waiter writer and reservation admission fail their
existing policy-mode checks. It does not yet fence every VM creation effect;
runtime wiring and final-off transitions remain deliberately absent.
"""

from dataclasses import dataclass
import json
from typing import Literal

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_policy import (
    EnforcementResourcePolicySnapshot,
    validate_enforcement_resource_policy_snapshot,
)


PolicyMode = Literal["off", "shadow", "enforce", "drain"]


@dataclass(frozen=True, slots=True)
class ResourcePolicyReceipt:
    cluster_id: str
    namespace: str
    policy_digest: str
    revision: int
    mode: PolicyMode


def _canonical(value) -> bytes:
    if isinstance(value, str):
        value = json.loads(value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _valid_receipt(value) -> bool:
    return (
        type(value) is ResourcePolicyReceipt
        and isinstance(value.cluster_id, str)
        and isinstance(value.namespace, str)
        and isinstance(value.policy_digest, str)
        and type(value.revision) is int
        and 1 <= value.revision < 2**63
        and type(value.mode) is str
        and value.mode in {"off", "shadow", "enforce", "drain"}
    )


class VMResourcePolicyLifecycleStore:
    """Own explicit lifecycle transitions without creating runtime authority."""

    def __init__(self, db, *, snapshot):
        self.db = db
        self.snapshot: EnforcementResourcePolicySnapshot = (
            validate_enforcement_resource_policy_snapshot(snapshot)
        )

    def _receipt(self, row) -> ResourcePolicyReceipt:
        try:
            if (
                row is None
                or row["cluster_id"] != self.snapshot.inventory.cluster_id
                or row["namespace"] != self.snapshot.inventory.namespace
                or row["policy_digest"] != self.snapshot.policy_digest
                or _canonical(row["document"]) != self.snapshot.canonical_document
                or type(row["revision"]) is not int
                or not 1 <= row["revision"] < 2**63
                or row["mode"] not in {"off", "shadow", "enforce", "drain"}
            ):
                raise ValueError
            return ResourcePolicyReceipt(
                row["cluster_id"],
                row["namespace"],
                row["policy_digest"],
                row["revision"],
                row["mode"],
            )
        except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
            raise ResourceAdmissionError("resource_policy_changed") from None

    async def ensure_shadow(self) -> ResourcePolicyReceipt:
        """Install once, or replay only the same already-shadow policy."""
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) "
                "VALUES($1,$2,$3,$4::jsonb,'shadow') "
                "ON CONFLICT (cluster_id) DO NOTHING RETURNING *",
                self.snapshot.inventory.cluster_id,
                self.snapshot.inventory.namespace,
                self.snapshot.policy_digest,
                self.snapshot.canonical_document.decode(),
            )
            if row is None:
                row = await conn.fetchrow(
                    "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                    self.snapshot.inventory.cluster_id,
                )
            receipt = self._receipt(row)
            if receipt.mode != "shadow":
                raise ResourceAdmissionError("resource_policy_changed")
            return receipt

    async def begin_drain(self, *, expected) -> ResourcePolicyReceipt:
        """Move one exact shadow/enforce epoch to drain, or replay its receipt."""
        if not _valid_receipt(expected):
            raise ResourceAdmissionError("resource_policy_changed")
        async with self.db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                self.snapshot.inventory.cluster_id,
            )
            current = self._receipt(row)
            if current != expected:
                raise ResourceAdmissionError("resource_policy_changed")
            if current.mode == "drain":
                return current
            if current.mode not in {"shadow", "enforce"}:
                raise ResourceAdmissionError("resource_policy_changed")
            row = await conn.fetchrow(
                "UPDATE vm_resource_admission_policy "
                "SET mode='drain',revision=revision+1 "
                "WHERE cluster_id=$1 AND revision=$2 RETURNING *",
                current.cluster_id,
                current.revision,
            )
            if row is None:
                raise ResourceAdmissionError("resource_policy_changed")
            return self._receipt(row)
