"""Inert boundary check for a near-expiry creation effect grant."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)


class _GrantConnection:
    def __init__(self, row, now, *, expire_at_refresh=False):
        self.row = row
        self.now = now
        self.expire_at_refresh = expire_at_refresh
        self.effects = 0

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def fetchrow(self, query, *args):
        if query.startswith("SELECT * FROM vm_creation_effects"):
            return None
        if query.startswith("WITH clock AS MATERIALIZED"):
            if self.expire_at_refresh:
                self.now += timedelta(seconds=3)
            if (
                self.row["request_id"] != args[0]
                or self.row["provision_generation"] != args[5]
                or self.row["claim_token"] != args[6]
                or self.row["state"] != "reconciling"
                or self.row["claim_expires_at"] <= self.now
                or self.row["admission_deadline"] <= self.now
            ):
                return None
            self.row["claim_expires_at"] = min(
                self.now + timedelta(seconds=60), self.row["admission_deadline"]
            )
            return {"claim_expires_at": self.row["claim_expires_at"]}
        raise AssertionError(query)

    async def fetchval(self, query, *args):
        if query == "SELECT clock_timestamp()":
            return self.now
        raise AssertionError(query)

    async def execute(self, query, *args):
        if query.startswith("UPDATE vm_creation_retries SET creation_carrier_uid="):
            return "UPDATE 1"
        if query.startswith("INSERT INTO vm_creation_effects"):
            self.effects += 1
            return "INSERT 0 1"
        raise AssertionError(query)


class _GrantDB:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_at_refresh", [False, True])
async def test_new_effect_refreshes_only_still_valid_exact_claim(
    monkeypatch, expire_at_refresh
):
    from orchestrator.services import vm_resource_job_runtime

    now = datetime.now(timezone.utc)
    request_id, generation, token = uuid4(), uuid4(), uuid4()
    row = {
        "request_id": request_id,
        "provision_generation": generation,
        "claim_token": token,
        "claim_expires_at": now + timedelta(seconds=2),
        "admission_deadline": now + timedelta(hours=1),
        "state": "reconciling",
        "canonical_request": {},
        "controller_configuration": {"golden_enabled": False},
    }
    conn = _GrantConnection(row, now, expire_at_refresh=expire_at_refresh)
    store = VMCreationRetryStore(_GrantDB(conn))
    effect_nonce = uuid4()
    values = {
        "version": 1,
        "effect_kind": "rootdisk",
        "effect_nonce": str(effect_nonce),
        "current_pvc_uid": None,
        "expected_pvc_uid": None,
        "current_dv_uid": None,
        "retained_dv_uid": None,
    }
    carrier = {"metadata": {"uid": str(uuid4()), "namespace": "agent-vms"}}

    async def scope(_conn, _request_id):
        assert _request_id == str(request_id)
        return row, {}

    async def no_op(*args, **kwargs):
        return None

    monkeypatch.setattr(store, "_carrier", lambda _carrier: values)
    monkeypatch.setattr(store, "_effect_scope", scope)
    monkeypatch.setattr(store, "_current", no_op)
    monkeypatch.setattr(store, "_check_carrier", no_op)
    monkeypatch.setattr(vm_resource_job_runtime, "installed_job_resource_store", no_op)

    if expire_at_refresh:
        with pytest.raises(VMCreationRetryConflict, match="retry_claim_changed"):
            await store.begin_effect(
                request_id=str(request_id), claim_token=str(token), carrier=carrier
            )
        assert row["claim_expires_at"] == now + timedelta(seconds=2)
        assert conn.effects == 0
    else:
        grant = await store.begin_effect(
            request_id=str(request_id), claim_token=str(token), carrier=carrier
        )
        assert grant["actuation_allowed"] is True
        assert grant["effect_nonce"] == str(effect_nonce)
        assert row["claim_expires_at"] == now + timedelta(seconds=60)
        assert conn.effects == 1
    assert row["claim_token"] == token
