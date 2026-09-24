"""Explicit receipt-bound transitions for the installed VM resource policy.

Run inside the configured orchestrator environment. The policy file contains
the exact reviewed full-capability document; this command never derives a
budget, enables a flag, or installs a policy as a startup side effect.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path

from orchestrator.services.vm_resource_policy_lifecycle_store import (
    ResourcePolicyReceipt,
    VMResourcePolicyLifecycleStore,
)
from shared.vm_inventory_transport import decode_document
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_inventory import InventoryError
from shared.vm_resource_policy import validate_enforcement_resource_policy


_ACTIONS = (
    "current-receipt", "ensure-shadow", "activate-enforce", "begin-drain",
    "finalize-off",
)
_RECEIPT_FIELDS = {
    "cluster_id", "namespace", "policy_digest", "revision", "mode",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=_ACTIONS)
    parser.add_argument("--policy-file", type=Path, required=True)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--policy-digest", required=True)
    parser.add_argument("--expected-receipt-file", type=Path)
    return parser


def load_request(args: argparse.Namespace):
    """Validate all caller input before opening a database connection."""
    try:
        document = decode_document(args.policy_file.read_bytes(), max_bytes=16384)
        snapshot = validate_enforcement_resource_policy(document)
    except (OSError, InventoryError, ResourceAdmissionError):
        raise ResourceAdmissionError("invalid_resource_policy") from None
    if (
        snapshot.inventory.protocol != 2
        or args.cluster_id != snapshot.inventory.cluster_id
        or args.policy_digest != snapshot.policy_digest
    ):
        raise ResourceAdmissionError("resource_policy_changed")
    if args.action in {"current-receipt", "ensure-shadow"}:
        if args.expected_receipt_file is not None:
            raise ResourceAdmissionError("resource_policy_changed")
        return snapshot, None
    if args.expected_receipt_file is None:
        raise ResourceAdmissionError("resource_policy_changed")
    try:
        value = decode_document(
            args.expected_receipt_file.read_bytes(), max_bytes=4096,
        )
        if (
            not isinstance(value, dict)
            or set(value) != _RECEIPT_FIELDS
            or value["cluster_id"] != snapshot.inventory.cluster_id
            or value["namespace"] != snapshot.inventory.namespace
            or value["policy_digest"] != snapshot.policy_digest
            or type(value["revision"]) is not int
            or not 1 <= value["revision"] < 2**63
            or value["mode"] not in {"shadow", "enforce", "drain"}
        ):
            raise ValueError
        expected = ResourcePolicyReceipt(**value)
    except (OSError, InventoryError, TypeError, ValueError, KeyError):
        raise ResourceAdmissionError("resource_policy_changed") from None
    return snapshot, expected


async def apply_transition(db, snapshot, *, action: str, expected):
    lifecycle = VMResourcePolicyLifecycleStore(db, snapshot=snapshot)
    if action == "current-receipt" and expected is None:
        return await lifecycle.current_receipt()
    if action == "ensure-shadow" and expected is None:
        return await lifecycle.ensure_shadow()
    if not isinstance(expected, ResourcePolicyReceipt):
        raise ResourceAdmissionError("resource_policy_changed")
    if action == "activate-enforce":
        return await lifecycle.activate_enforce(expected=expected)
    if action == "begin-drain":
        return await lifecycle.begin_drain(expected=expected)
    if action == "finalize-off":
        return await lifecycle.finalize_off(expected=expected)
    raise ResourceAdmissionError("resource_policy_changed")


async def run(args: argparse.Namespace, *, db_factory=None) -> dict:
    from orchestrator.database.postgres import PostgresDB

    snapshot, expected = load_request(args)
    db = (
        db_factory() if db_factory is not None
        else PostgresDB(min_connections=1, max_connections=2)
    )
    await db.connect()
    try:
        receipt = await apply_transition(
            db, snapshot, action=args.action, expected=expected,
        )
    finally:
        await db.close()
    return asdict(receipt)


def main() -> None:
    args = build_parser().parse_args()
    try:
        receipt = asyncio.run(run(args))
    except Exception as exc:
        # Database/configuration errors can contain endpoint or credential data.
        print(json.dumps({
            "error": "resource-policy-transition-failed",
            "category": type(exc).__name__,
        }, sort_keys=True))
        raise SystemExit(1) from None
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
