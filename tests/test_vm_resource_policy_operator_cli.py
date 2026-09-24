"""Operator transitions use one exact policy and prior receipt on real PG."""

from dataclasses import asdict
import json
from pathlib import Path
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_resource_policy_lifecycle_store_real_postgres import (
    db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
)
from tests.test_vm_resource_policy import whole_launcher_policy


def snapshot():
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    document = whole_launcher_policy()
    document["namespace"] = "workers"
    document["policy"].update(
        stableClusterId=f"d3i-{uuid4()}", shadowEnabled=True,
        enforcementEnabled=True,
    )
    return validate_enforcement_resource_policy(document)


def _arguments(tmp_path, policy, action, receipt=None):
    from orchestrator.operator_cli.vm_resource_policy import build_parser

    policy_file = tmp_path / "resource-policy.json"
    policy_file.write_bytes(policy.canonical_document)
    arguments = [
        action, "--policy-file", str(policy_file),
        "--cluster-id", policy.inventory.cluster_id,
        "--policy-digest", policy.policy_digest,
    ]
    if receipt is not None:
        receipt_file = tmp_path / "resource-receipt.json"
        receipt_file.write_text(json.dumps(asdict(receipt)))
        arguments.extend(["--expected-receipt-file", str(receipt_file)])
    return build_parser().parse_args(arguments)


def test_operator_input_binds_complete_document_identity_and_receipt(tmp_path):
    from orchestrator.operator_cli.vm_resource_policy import load_request

    policy = snapshot()
    args = _arguments(tmp_path, policy, "ensure-shadow")
    loaded, expected = load_request(args)
    assert loaded == policy
    assert expected is None
    args.cluster_id = "different-cluster"
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        load_request(args)
    args.cluster_id = policy.inventory.cluster_id
    args.policy_digest = "sha256:" + "0" * 64
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        load_request(args)
    args.policy_digest = policy.policy_digest
    args.action = "activate-enforce"
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        load_request(args)


def test_operator_input_refuses_duplicate_policy_key_and_foreign_receipt(tmp_path):
    from orchestrator.operator_cli.vm_resource_policy import load_request
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        ResourcePolicyReceipt,
    )

    policy = snapshot()
    foreign = ResourcePolicyReceipt(
        "foreign", policy.inventory.namespace, policy.policy_digest, 1, "shadow",
    )
    args = _arguments(tmp_path, policy, "activate-enforce", foreign)
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        load_request(args)
    args = _arguments(tmp_path, policy, "ensure-shadow")
    Path(args.policy_file).write_text(
        policy.canonical_document.decode().replace(
            '"mode":"same-cluster"',
            '"mode":"same-cluster","mode":"same-cluster"',
        )
    )
    with pytest.raises(ResourceAdmissionError, match="invalid_resource_policy"):
        load_request(args)


@pytest.mark.asyncio
async def test_operator_transitions_require_receipts_and_preserve_shadow_on_refusal(
    db, tmp_path,  # noqa: F811
):
    from orchestrator.operator_cli.vm_resource_policy import (
        apply_transition, load_request,
    )

    policy = snapshot()
    loaded, expected = load_request(_arguments(tmp_path, policy, "ensure-shadow"))
    shadow = await apply_transition(db, loaded, action="ensure-shadow", expected=expected)
    assert shadow.mode == "shadow" and shadow.revision == 1
    assert await apply_transition(
        db, loaded, action="ensure-shadow", expected=None,
    ) == shadow

    loaded, expected = load_request(
        _arguments(tmp_path, policy, "activate-enforce", shadow)
    )
    with pytest.raises(ResourceAdmissionError, match="inventory_unavailable"):
        await apply_transition(db, loaded, action="activate-enforce", expected=expected)
    row = await db.fetchrow(
        "SELECT mode,revision FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    )
    assert (row["mode"], row["revision"]) == ("shadow", 1)

    loaded, expected = load_request(
        _arguments(tmp_path, policy, "begin-drain", shadow)
    )
    drained = await apply_transition(
        db, loaded, action="begin-drain", expected=expected,
    )
    assert drained.mode == "drain" and drained.revision == 2
    with pytest.raises(ResourceAdmissionError, match="resource_policy_changed"):
        await apply_transition(
            db, loaded, action="begin-drain", expected=shadow,
        )
    with pytest.raises(ResourceAdmissionError, match="inventory_unavailable"):
        await apply_transition(
            db, loaded, action="finalize-off", expected=drained,
        )


@pytest.mark.asyncio
async def test_operator_command_uses_application_connection_and_returns_only_receipt(
    db, pg_dsn, tmp_path,  # noqa: F811
):
    from orchestrator.database.postgres import PostgresDB
    from orchestrator.operator_cli.vm_resource_policy import run

    policy = snapshot()
    args = _arguments(tmp_path, policy, "ensure-shadow")
    result = await run(
        args,
        db_factory=lambda: PostgresDB(
            connection_string=pg_dsn, min_connections=1, max_connections=2,
        ),
    )
    assert result == {
        "cluster_id": policy.inventory.cluster_id,
        "namespace": policy.inventory.namespace,
        "policy_digest": policy.policy_digest,
        "revision": 1,
        "mode": "shadow",
    }
    assert await db.fetchval(
        "SELECT mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
        policy.inventory.cluster_id,
    ) == "shadow"


def test_operator_command_redacts_database_failure(monkeypatch, capsys, tmp_path):
    from orchestrator.operator_cli import vm_resource_policy as command

    policy = snapshot()
    args = _arguments(tmp_path, policy, "ensure-shadow")
    monkeypatch.setattr(
        sys, "argv", [
            "vm_resource_policy", args.action,
            "--policy-file", str(args.policy_file),
            "--cluster-id", args.cluster_id,
            "--policy-digest", args.policy_digest,
        ],
    )
    monkeypatch.setattr(
        command, "run", AsyncMock(side_effect=RuntimeError("password=hidden")),
    )
    with pytest.raises(SystemExit) as exit_status:
        command.main()
    assert exit_status.value.code == 1
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "error": "resource-policy-transition-failed",
        "category": "RuntimeError",
    }
    assert "hidden" not in output
