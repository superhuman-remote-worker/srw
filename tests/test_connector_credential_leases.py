"""Credential leases (connector drivers slice C2) without a database.

Tokens and their redaction, the spec flag and the lease probe driver, the
exchange's decision table and its dedicated port, the sweeper loop and its
registration, and the agent's lease materializer. The SQL is proven in
``test_connector_credential_leases_real_postgres.py``; the issue and revoke
points in ``test_connector_lease_delivery_wiring.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import re
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from agent.connectors import RuntimeContext, deliveries_from_payload
from agent.connectors.lease import LeaseTokenMaterializer
from orchestrator.application import background_tasks
from orchestrator.application import connectors as connectors_composition
from orchestrator.application.settings import parse_exchange_port
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
    SupportsCredentialLease,
)
from orchestrator.services.connector_drivers.lease_probe import LeaseProbeDriver
from orchestrator.services.connector_lease_exchange import (
    EXCHANGE_PATH,
    INTROSPECT_PATH,
    DenialLimiter,
    ExchangeOutcome,
    check_denial,
)
from shared import content_redaction, logging_format
from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    GENERIC_SPEC,
    LEASE_PROBE_SPEC,
    LEGACY_TYPE_IDS,
    spec_for_type,
)
from shared.connectors.contract import effective_access, validate_spec
from shared.connectors.leases import (
    MAX_CACHE_SECONDS,
    TOKEN_LENGTH,
    TOKEN_PATTERN,
    last_four,
    lease_file_name,
    mint_token,
    operation_allowed,
    token_digest,
    token_shape_valid,
)

CONNECTOR = "00000000-0000-4000-8000-0000000000c3"


# =============================================================================
# Tokens
# =============================================================================


class TestTokens:
    @pytest.mark.parametrize("prefix", ["scl", "sdi"])
    def test_a_minted_token_has_its_prefix_length_and_checksum(self, prefix):
        token = mint_token(prefix)
        assert token.startswith(prefix + "_")
        assert len(token) == TOKEN_LENGTH == 53
        assert re.fullmatch(r"s(?:cl|di)_[0-9A-Za-z]{49}", token)
        assert token_shape_valid(token, prefix)
        assert not token_shape_valid(token, "sdi" if prefix == "scl" else "scl")

    def test_tokens_are_random(self):
        assert len({mint_token("scl") for _ in range(50)}) == 50

    def test_a_changed_character_fails_the_checksum(self):
        token = mint_token("scl")
        index = 10
        flipped = "A" if token[index] != "A" else "B"
        assert not token_shape_valid(
            token[:index] + flipped + token[index + 1 :], "scl"
        )

    @pytest.mark.parametrize(
        "value", [None, 42, "", "scl_short", "srw_" + "a" * 49, "scl_" + "!" * 49]
    )
    def test_malformed_values_are_not_tokens(self, value):
        assert not token_shape_valid(value, "scl")

    def test_an_unknown_prefix_cannot_be_minted(self):
        with pytest.raises(ValueError):
            mint_token("srw")

    def test_the_digest_is_sha256_and_last_four_is_the_tail(self):
        token = mint_token("scl")
        assert token_digest(token) == hashlib.sha256(token.encode()).digest()
        assert last_four(token) == token[-4:]

    def test_the_lease_file_is_named_by_the_connector_uuid(self):
        assert lease_file_name(CONNECTOR.upper()) == (
            f".srw-credentials/leases/{CONNECTOR}"
        )
        for bad in ("../etc/passwd", "abc", ""):
            with pytest.raises(ValueError):
                lease_file_name(bad)

    def test_the_scanner_pattern_matches_both_kinds(self):
        text = f"a {mint_token('scl')} b {mint_token('sdi')} c srw_{'a' * 49}"
        assert len(re.findall(TOKEN_PATTERN, text)) == 2


class TestRedaction:
    @pytest.mark.parametrize(
        "sanitize",
        [content_redaction.sanitize, content_redaction.sanitize_tool_output],
        ids=["presentation", "tool"],
    )
    @pytest.mark.parametrize("prefix", ["scl", "sdi"])
    def test_content_redaction_removes_lease_and_identity_tokens(
        self, sanitize, prefix
    ):
        token = mint_token(prefix)
        result = sanitize(f"cat ~/.srw-credentials/leases/x -> {token} done")
        assert token not in result.text
        assert result.count == 1
        assert result.text.endswith(" done")

    @pytest.mark.parametrize("prefix", ["scl", "sdi"])
    def test_log_redaction_removes_lease_and_identity_tokens(self, prefix):
        token = mint_token(prefix)
        assert token not in logging_format.redact(f"exchanged {token} ok")

    def test_ordinary_words_with_the_prefix_survive(self):
        text = "scl_config and sdi_mode are identifiers, not tokens"
        assert content_redaction.sanitize_tool_output(text).text == text
        assert logging_format.redact(text) == text


# =============================================================================
# The spec flag and the probe driver
# =============================================================================


class TestSpec:
    def test_no_shipped_datasource_driver_delivers_by_lease(self):
        assert all(spec.credential_delivery == "inline" for spec in DATASOURCE_SPECS)

    def test_the_probe_is_a_valid_lease_driver_outside_every_catalogue(self):
        assert validate_spec(LEASE_PROBE_SPEC) == []
        assert LEASE_PROBE_SPEC.credential_delivery == "lease"
        assert spec_for_type("lease_probe") is LEASE_PROBE_SPEC
        assert "lease_probe" not in LEGACY_TYPE_IDS
        assert LEASE_PROBE_SPEC not in DATASOURCE_SPECS

    def test_the_lease_flag_and_the_lease_token_form_go_together(self):
        without_form = dataclasses.replace(
            LEASE_PROBE_SPEC, delivery_forms=("env_file",)
        )
        form_without_flag = dataclasses.replace(
            GENERIC_SPEC,
            delivery_forms=(
                "env_file",
                "lease_token",
            ),
        )
        unknown = dataclasses.replace(GENERIC_SPEC, credential_delivery="vault")
        for spec in (without_form, form_without_flag):
            assert any("lease_token" in p for p in validate_spec(spec))
        assert any("credential_delivery" in p for p in validate_spec(unknown))

    def test_a_lease_is_issued_at_the_level_the_agent_binds(self):
        """One rule on both sides: the agent's binding ``access`` and the
        level a lease is issued at are the same function, for every spec and
        every config shape (an unknown level fails closed to the lowest)."""
        from agent.connectors.legacy import binding_from_legacy_entry
        from agent.connectors.legacy import effective_access as agent_rule
        from shared.connectors.builtin import BUILTIN_SPECS, DEVELOPMENT_SPECS

        assert agent_rule is effective_access
        shapes = [
            {},
            {"project_read_only": True},
            {"project_read_only": False, "config": {"access": "send"}},
            {"config": {"access": "ReadOnly"}},
            {"config": {"access": "no-such-level"}},
            {"config": "not a mapping"},
        ]
        for spec in BUILTIN_SPECS + DEVELOPMENT_SPECS:
            for shape in shapes:
                entry = {"type": spec.legacy_type, "name": "x", **shape}
                expected = effective_access(entry, spec)
                levels = spec.ranked_access_ids()
                assert expected is None if not levels else expected in levels
                if spec.legacy_type:
                    binding = binding_from_legacy_entry(entry)
                    assert binding is not None and binding.access == expected
        assert effective_access({}, LEASE_PROBE_SPEC) == "ReadWrite"
        assert effective_access({"project_read_only": True}, LEASE_PROBE_SPEC) == (
            "ReadOnly"
        )

    @pytest.mark.parametrize(
        ("operation", "access", "allowed"),
        [
            ("read", "ReadOnly", True),
            ("read", "ReadWrite", True),
            ("write", "ReadOnly", False),
            ("write", "ReadWrite", True),
            ("delete", "ReadWrite", False),
            ("read", None, False),
        ],
    )
    def test_operation_allowed(self, operation, access, allowed):
        assert operation_allowed(operation, access) is allowed

    def test_needs_leases_looks_only_at_lease_drivers(self):
        assert not leases.needs_leases(None)
        assert not leases.needs_leases([{"type": "generic"}, "junk"])
        assert leases.needs_leases([{"type": "generic"}, {"type": "lease_probe"}])


class TestRegistry:
    def test_the_probe_is_installed_only_when_asked(self):
        assert builtin_connector_drivers().for_type("lease_probe") is None
        registry = builtin_connector_drivers(lease_probe=True)
        driver = registry.for_type("lease_probe")
        assert isinstance(driver, LeaseProbeDriver)
        assert isinstance(driver, SupportsCredentialLease)
        assert registry.drivers()[-1] is driver


def _draft(**over: Any) -> ConnectorDraft:
    fields: dict[str, Any] = dict(
        name="Probe",
        connection_url=None,
        credentials=None,
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    fields.update(over)
    return ConnectorDraft(**fields)


class TestProbeDriver:
    @pytest.mark.asyncio
    async def test_create_stores_the_secret_and_the_upstream(self):
        normalized = await LeaseProbeDriver().validate(
            _draft(
                credentials={"secret": "s3cret"},
                config={"upstream": "https://u.invalid"},
            ),
            existing=None,
            ctx=MagicMock(),
        )
        assert normalized.credentials == {"secret": "s3cret"}
        assert normalized.config == {"upstream": "https://u.invalid"}
        assert normalized.connection_url is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "over",
        [
            {"credentials": None},
            {"credentials": {"secret": ""}},
            {"credentials": {"secret": "x", "extra": "y"}},
            {"credentials": {"secret": "x"}, "config": {"other": 1}},
            {"credentials": {"secret": "x"}, "connection_url": "https://x"},
        ],
    )
    async def test_create_refuses_anything_else(self, over):
        with pytest.raises(HTTPException) as exc:
            await LeaseProbeDriver().validate(
                _draft(**over), existing=None, ctx=MagicMock()
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_a_blank_edit_keeps_the_stored_secret(self):
        normalized = await LeaseProbeDriver().validate(
            _draft(credentials={}), existing={"id": CONNECTOR}, ctx=MagicMock()
        )
        assert normalized.credentials is None

    def test_bind_never_carries_the_secret(self):
        row = {
            "id": CONNECTOR,
            "type": "lease_probe",
            "name": "Probe",
            "credentials": {"secret": "s3cret"},
            "project_read_only": True,
        }
        ctx = BindContext(
            gates=DeploymentGates(lambda: False, lambda: False),
            logger=MagicMock(),
            default_known_hosts="",
        )
        entry = LeaseProbeDriver().bind(row, row["credentials"], ctx=ctx)
        assert entry["credentials"] == {}
        assert entry["datasource_id"] == CONNECTOR
        assert entry["project_read_only"] is True
        assert "s3cret" not in repr(entry)

    def test_the_upstream_is_the_secret_and_the_configured_destination(self):
        driver = LeaseProbeDriver()
        assert driver.lease_upstream(
            {"credentials": {"secret": "s"}, "config": {"upstream": "https://u"}}
        ) == {"credential": "s", "allowed_upstream": ["https://u"]}
        assert (
            driver.lease_upstream({"credentials": {"secret": "s"}, "config": {}})[
                "allowed_upstream"
            ]
            == []
        )
        with pytest.raises(ValueError):
            driver.lease_upstream({"credentials": {}, "config": {}})


# =============================================================================
# The exchange decision and its port
# =============================================================================


def _row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "identity_id": "i-1",
        "identity_connector_id": CONNECTOR,
        "identity_driver": "srw.lease-probe/v1",
        "identity_revoked": False,
        "lease_id": "l-1",
        "connector_id": CONNECTOR,
        "driver": "srw.lease-probe/v1",
        "access": "ReadOnly",
        "lease_revoked": False,
        "revoke_reason": None,
        "lease_unexpired": True,
    }
    row.update(over)
    return row


class TestCheckDenial:
    @pytest.mark.parametrize(
        ("over", "operation", "reason"),
        [
            ({}, "read", None),
            ({}, "write", "operation_not_allowed"),
            ({"identity_id": None}, "read", "unknown_driver_identity"),
            ({"identity_revoked": True}, "read", "driver_identity_revoked"),
            ({"lease_id": None}, "read", "unknown_lease"),
            (
                {"identity_connector_id": "other"},
                "read",
                "driver_identity_of_another_connector",
            ),
            (
                {"identity_driver": "srw.other/v1"},
                "read",
                "driver_identity_of_another_connector",
            ),
            ({"lease_revoked": True, "revoke_reason": "x"}, "read", "lease_revoked"),
            (
                {"lease_revoked": True, "revoke_reason": "expired"},
                "read",
                "lease_expired",
            ),
            ({"lease_unexpired": False}, "read", "lease_expired"),
            ({"lease_revoked": True}, None, None),
            (
                {"identity_connector_id": "other"},
                None,
                "driver_identity_of_another_connector",
            ),
        ],
    )
    def test_the_decision_table(self, over, operation, reason):
        assert check_denial(_row(**over), operation=operation) == reason

    def test_no_row_is_an_unknown_identity(self):
        assert check_denial(None, operation="read") == "unknown_driver_identity"


class _FakeExchange:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def exchange(self, **kwargs: Any) -> ExchangeOutcome:
        self.calls.append(("exchange", kwargs))
        if kwargs["identity_token"] != "sdi_ok":
            return ExchangeOutcome(401, {"error": "unknown_driver_identity"})
        return ExchangeOutcome(200, {"credential": "c", "max_cache_seconds": 30})

    async def introspect(self, **kwargs: Any) -> ExchangeOutcome:
        self.calls.append(("introspect", kwargs))
        return ExchangeOutcome(200, {"active": False})


@pytest.fixture
def exchange_client(monkeypatch):
    fake = _FakeExchange()
    monkeypatch.setattr(
        connectors_composition, "connector_lease_exchange", lambda *_args: fake
    )
    app = connectors_composition.connector_lease_exchange_app(SimpleNamespace())
    return TestClient(app), fake


class TestExchangePort:
    def test_success_and_denial_are_never_cached(self, exchange_client):
        client, fake = exchange_client
        ok = client.post(
            EXCHANGE_PATH,
            json={"lease_token": "scl_x", "operation": "read"},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        denied = client.post(
            EXCHANGE_PATH,
            json={"lease_token": "scl_x", "operation": "write"},
            headers={"Authorization": "Bearer sdi_bad"},
        )
        assert ok.status_code == 200 and ok.json()["credential"] == "c"
        assert denied.status_code == 401
        for response in (ok, denied):
            assert response.headers["cache-control"] == "no-store"
        assert fake.calls[0][1]["identity_token"] == "sdi_ok"
        assert fake.calls[0][1]["operation"] == "read"

    def test_a_missing_or_non_bearer_identity_is_empty(self, exchange_client):
        client, fake = exchange_client
        client.post(EXCHANGE_PATH, json={"lease_token": "t", "operation": "read"})
        client.post(
            EXCHANGE_PATH,
            json={"lease_token": "t", "operation": "read"},
            headers={"Authorization": "Basic sdi_ok"},
        )
        assert [call[1]["identity_token"] for call in fake.calls] == ["", ""]

    def test_an_invalid_body_is_refused_without_echoing_it(self, exchange_client):
        client, fake = exchange_client
        response = client.post(
            EXCHANGE_PATH, json={"lease_token": "scl_secretvalue", "operation": "rm"}
        )
        assert response.status_code == 422
        assert "scl_secretvalue" not in response.text
        assert response.headers["cache-control"] == "no-store"
        assert fake.calls == []

    def test_introspection_is_served(self, exchange_client):
        client, _ = exchange_client
        response = client.post(INTROSPECT_PATH, json={"lease_token": "t"})
        assert response.json() == {"active": False}

    def test_the_port_serves_nothing_else(self, exchange_client):
        client, _ = exchange_client
        app = client.app
        paths = {getattr(route, "path", None) for route in app.routes}
        assert paths == {EXCHANGE_PATH, INTROSPECT_PATH}
        for path in ("/docs", "/openapi.json", "/api/health"):
            assert client.get(path).status_code == 404

    def test_the_main_application_never_routes_the_exchange(self):
        from orchestrator.application import create_app

        app = create_app()
        paths = {getattr(route, "path", "") for route in app.routes}
        assert EXCHANGE_PATH not in paths and INTROSPECT_PATH not in paths
        assert not any("connector-lease" in path for path in paths)

    def test_max_cache_seconds_is_thirty(self):
        assert MAX_CACHE_SECONDS == 30


class TestExchangePortSetting:
    @pytest.mark.parametrize(
        ("raw", "port"),
        [
            (None, None),
            ("", None),
            ("0", None),
            ("8088", 8088),
            (" 9001 ", 9001),
            ("8085", None),
            ("http", None),
            ("70000", None),
        ],
    )
    def test_parse(self, raw, port):
        assert parse_exchange_port(raw) == port


class TestServer:
    @pytest.mark.asyncio
    async def test_a_busy_port_leaves_the_exchange_off_not_the_process(self):
        import socket

        holder = socket.socket()
        holder.bind(("0.0.0.0", 0))
        holder.listen()
        port = holder.getsockname()[1]
        try:
            await asyncio.wait_for(
                connectors_composition.serve_connector_lease_exchange(
                    SimpleNamespace(), port=port, shutdown_event=asyncio.Event()
                ),
                timeout=5,
            )
        finally:
            holder.close()

    @pytest.mark.asyncio
    async def test_it_serves_until_shutdown(self, monkeypatch):
        import socket

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        fake = _FakeExchange()
        monkeypatch.setattr(
            connectors_composition, "connector_lease_exchange", lambda *_args: fake
        )
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            connectors_composition.serve_connector_lease_exchange(
                SimpleNamespace(), port=port, shutdown_event=shutdown
            )
        )
        import httpx

        async with httpx.AsyncClient() as client:
            for _ in range(50):
                try:
                    response = await client.post(
                        f"http://127.0.0.1:{port}{INTROSPECT_PATH}",
                        json={"lease_token": "t"},
                    )
                    break
                except httpx.ConnectError:
                    await asyncio.sleep(0.05)
        assert response.json() == {"active": False}
        shutdown.set()
        await asyncio.wait_for(task, timeout=5)


# =============================================================================
# The sweeper
# =============================================================================


class TestSweeper:
    @pytest.mark.asyncio
    async def test_a_pass_renews_then_retires_and_stops_on_shutdown(self, monkeypatch):
        calls: list[str] = []
        conn = object()

        @contextlib.asynccontextmanager
        async def acquire():
            yield conn

        async def renew(c):
            assert c is conn
            calls.append("renew")
            return 1

        async def retire(c):
            calls.append("retire")
            shutdown.set()
            return 0

        monkeypatch.setattr(leases, "renew_live_leases", renew)
        monkeypatch.setattr(leases, "retire_expired_leases", retire)
        shutdown = asyncio.Event()
        await asyncio.wait_for(
            leases.connector_lease_sweeper(
                shutdown, store=SimpleNamespace(acquire=acquire), interval_seconds=0.01
            ),
            timeout=5,
        )
        assert calls == ["renew", "retire"]

    @pytest.mark.asyncio
    async def test_a_failed_pass_does_not_end_the_loop(self, monkeypatch):
        attempts = 0

        @contextlib.asynccontextmanager
        async def acquire():
            nonlocal attempts
            attempts += 1
            if attempts >= 2:
                shutdown.set()
            raise RuntimeError("db down")
            yield

        shutdown = asyncio.Event()
        await asyncio.wait_for(
            leases.connector_lease_sweeper(
                shutdown, store=SimpleNamespace(acquire=acquire), interval_seconds=0.01
            ),
            timeout=5,
        )
        assert attempts == 2

    def test_the_interval_keeps_a_renewal_in_every_second_half(self):
        assert leases.SWEEP_INTERVAL_SECONDS <= leases.LEASE_TTL_SECONDS / 4
        assert leases.RENEW_BELOW_SECONDS == leases.LEASE_TTL_SECONDS // 2

    def test_both_tasks_have_a_shutdown_slot(self):
        order = background_tasks.BACKGROUND_TASK_SHUTDOWN_ORDER
        assert "connector_lease_sweeper" in order
        assert "connector_lease_exchange" in order


# =============================================================================
# The agent's lease materializer
# =============================================================================


def _probe_entry(token: str = "scl_test", connector: str = CONNECTOR) -> dict:
    return {
        "type": "lease_probe",
        "name": "Probe",
        "datasource_id": connector,
        "credentials": {
            "lease": {"id": "l-1", "connector_id": connector, "token": token}
        },
    }


class TestLeaseMaterializer:
    def _rt(self, backend: Any) -> RuntimeContext:
        return RuntimeContext(
            execution="session", workspace_manager=SimpleNamespace(backend=backend)
        )

    def test_the_token_is_installed_through_the_backend(self):
        backend = MagicMock(supports_shell=True)
        LeaseTokenMaterializer().materialize(
            deliveries_from_payload([_probe_entry()]), self._rt(backend)
        )
        backend.install_connector_lease.assert_called_once_with(CONNECTOR, "scl_test")

    def test_an_entry_without_a_lease_delivers_nothing(self):
        backend = MagicMock(supports_shell=True)
        entry = _probe_entry()
        entry["credentials"] = {}
        LeaseTokenMaterializer().materialize(
            deliveries_from_payload([entry]), self._rt(backend)
        )
        backend.install_connector_lease.assert_not_called()

    def test_a_workspace_without_a_shell_gets_nothing(self):
        backend = MagicMock(supports_shell=False)
        LeaseTokenMaterializer().materialize(
            deliveries_from_payload([_probe_entry()]), self._rt(backend)
        )
        backend.install_connector_lease.assert_not_called()

    def test_a_live_detach_removes_the_file_and_keeps_the_rest(self):
        other = "00000000-0000-4000-8000-0000000000d4"
        backend = MagicMock(supports_shell=True)
        old = deliveries_from_payload(
            [_probe_entry(), _probe_entry("scl_other", other)]
        )
        new = deliveries_from_payload([_probe_entry("scl_other", other)])
        LeaseTokenMaterializer().replace(old, new, self._rt(backend))
        backend.remove_connector_lease.assert_called_once_with(CONNECTOR)
        backend.install_connector_lease.assert_called_once_with(other, "scl_other")

    def test_a_backend_swap_installs_again(self):
        backend = MagicMock(supports_shell=True)
        LeaseTokenMaterializer().on_backend_swap(
            deliveries_from_payload([_probe_entry()]), backend
        )
        backend.install_connector_lease.assert_called_once_with(CONNECTOR, "scl_test")

    def test_the_readme_names_the_file_never_the_token(self):
        deliveries = deliveries_from_payload([_probe_entry()])
        lines = LeaseTokenMaterializer().facts(deliveries, self._rt(None))
        text = "\n".join(lines[0].lines)
        assert f"~/.srw-credentials/leases/{CONNECTOR}" in text
        assert "scl_test" not in text

    def test_the_binding_descriptor_carries_one_lease_token_entry(self):
        (delivery,) = deliveries_from_payload([_probe_entry()])
        (entry,) = delivery.binding.entries
        assert entry.form == "lease_token" and entry.recipient == "workspace"
        assert dict(entry.value) == {
            "lease_id": "l-1",
            "connector_id": CONNECTOR,
            "token": "scl_test",
        }
        assert "scl_test" not in repr(entry)


class TestRemoteBackendLeaseFile:
    def _backend(self) -> Any:
        from shared.runtime.core.backends.remote import RemoteBackend

        backend = object.__new__(RemoteBackend)
        backend._init_shell = MagicMock()
        backend._resolve_home_path = lambda rel: "/home/agent/" + rel
        backend.execute_claim_resource_with_secret_stdin = MagicMock(return_value=True)
        return backend

    def test_the_token_travels_on_stdin_only(self):
        backend = self._backend()
        path = backend.install_connector_lease(CONNECTOR, "scl_secret_token")
        command, secret = backend.execute_claim_resource_with_secret_stdin.call_args[0]
        assert secret == "scl_secret_token"
        assert "scl_secret_token" not in command
        assert path == f"/home/agent/.srw-credentials/leases/{CONNECTOR}"
        assert " install " in command and path in command

    def test_remove_sends_no_secret(self):
        backend = self._backend()
        backend.remove_connector_lease(CONNECTOR)
        command, secret = backend.execute_claim_resource_with_secret_stdin.call_args[0]
        assert secret == "" and " remove " in command

    def test_a_failed_install_raises(self):
        from shared.runtime.core.workspace_backend import WorkspaceUnavailableError

        backend = self._backend()
        backend.execute_claim_resource_with_secret_stdin.return_value = False
        with pytest.raises(WorkspaceUnavailableError):
            backend.install_connector_lease(CONNECTOR, "scl_x")

    def test_the_workspace_program_writes_a_0600_file(self, tmp_path):
        import os
        import subprocess
        import sys

        from shared.runtime.core.credential_env import CONNECTOR_LEASE_FILE

        target = tmp_path / ".srw-credentials" / "leases" / CONNECTOR
        subprocess.run(
            [sys.executable, "-c", CONNECTOR_LEASE_FILE, "install", str(target)],
            input="scl_file_token",
            text=True,
            check=True,
        )
        assert target.read_text() == "scl_file_token"
        assert os.stat(target).st_mode & 0o777 == 0o600
        assert os.stat(target.parent).st_mode & 0o777 == 0o700
        subprocess.run(
            [sys.executable, "-c", CONNECTOR_LEASE_FILE, "remove", str(target)],
            input="",
            text=True,
            check=True,
        )
        assert not target.exists()


# =============================================================================
# LeaseOwner
# =============================================================================


class TestLeaseOwner:
    def test_a_child_on_its_parents_workspace_is_owned_by_the_parent(self):
        parent = "00000000-0000-4000-8000-0000000000e1"
        child = {
            "id": "00000000-0000-4000-8000-0000000000e2",
            "parent_job_id": parent,
            "context": {"inherits_parent_workspace": True},
        }
        assert leases.job_lease_owner(child) == leases.LeaseOwner.job(parent)
        alone = dict(child, context={})
        assert leases.job_lease_owner(alone) == leases.LeaseOwner.job(child["id"])

    def test_a_session_workspace_owner_is_a_thread(self):
        owner = SimpleNamespace(kind="session", id="t-1")
        assert leases.LeaseOwner.of_workspace(owner) == leases.LeaseOwner(
            "thread", "t-1"
        )

    def test_an_unknown_kind_is_refused(self):
        with pytest.raises(ValueError):
            leases.LeaseOwner("pod", "x")

    @pytest.mark.asyncio
    async def test_a_payload_without_lease_entries_never_touches_the_store(self):
        store = SimpleNamespace(acquire=MagicMock(side_effect=AssertionError))
        assert (
            await leases.deliver_connector_leases_with(
                store, [{"type": "generic"}], owner=leases.LeaseOwner.thread("t")
            )
            == 0
        )

    @pytest.mark.asyncio
    async def test_the_backstop_never_raises(self):
        @contextlib.asynccontextmanager
        async def acquire():
            raise RuntimeError("down")
            yield

        await leases.revoke_terminal_execution_leases_with(
            SimpleNamespace(acquire=acquire), owner=leases.LeaseOwner.job("j")
        )

    @pytest.mark.asyncio
    async def test_revoke_needs_exactly_one_owner(self):
        with pytest.raises(ValueError):
            await leases.revoke_execution_leases(
                AsyncMock(), job_id="a", thread_id="b", reason="session_end"
            )


# =============================================================================
# Review hardening: the denial limiter, the bounded port, the settings
# =============================================================================


class TestDenialLimiter:
    def test_repeats_in_a_window_are_held_and_counted_on_the_next_record(self):
        now = [0.0]
        limiter = DenialLimiter(window_seconds=60, clock=lambda: now[0])
        key = ("identity-1", "lease_revoked")
        assert limiter.admit(key) == (True, 0)
        assert limiter.admit(key) == (False, 0)
        assert limiter.admit(key) == (False, 0)
        assert limiter.admit(("identity-1", "operation_not_allowed")) == (True, 0)
        now[0] = 61.0
        assert limiter.admit(key) == (True, 2)

    def test_the_key_table_is_bounded(self):
        now = [0.0]
        limiter = DenialLimiter(window_seconds=60, max_keys=2, clock=lambda: now[0])
        assert limiter.admit(("a", "r"))[0]
        assert limiter.admit(("b", "r"))[0]
        # Full of live keys: further identities share one overflow key.
        assert limiter.admit(("c", "r")) == (True, 0)
        assert limiter.admit(("d", "r")) == (False, 0)
        now[0] = 120.0
        assert limiter.admit(("e", "r")) == (True, 0)


async def _asgi_call(app, *, body_chunks, headers=()):
    sent: list[dict] = []
    chunks = list(body_chunks)

    async def receive():
        if chunks:
            chunk = chunks.pop(0)
            return {"type": "http.request", "body": chunk, "more_body": bool(chunks)}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "method": "POST",
        "path": EXCHANGE_PATH,
        "headers": [(k.encode(), v.encode()) for k, v in headers],
    }
    await app(scope, receive, send)
    return sent


async def _echo(scope, receive, send):
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body"):
            break
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": str(len(body)).encode()})


class TestBoundedPort:
    @pytest.mark.asyncio
    async def test_a_declared_oversized_body_is_refused_before_reading(self):
        read = []

        async def app(scope, receive, send):
            read.append(True)
            await _echo(scope, receive, send)

        sent = await _asgi_call(
            connectors_composition.BodyLimit(app, limit=16),
            body_chunks=[b"x" * 64],
            headers=[("content-length", "64")],
        )
        assert sent[0]["status"] == 413 and not read
        assert (b"cache-control", b"no-store") in sent[0]["headers"]

    @pytest.mark.asyncio
    async def test_a_streamed_body_past_the_limit_is_refused(self):
        sent = await _asgi_call(
            connectors_composition.BodyLimit(_echo, limit=16),
            body_chunks=[b"x" * 10, b"x" * 10, b"x" * 10],
        )
        assert sent[0]["status"] == 413

    @pytest.mark.asyncio
    async def test_a_small_body_passes(self):
        sent = await _asgi_call(
            connectors_composition.BodyLimit(_echo, limit=16),
            body_chunks=[b"x" * 8, b"x" * 8],
            headers=[("content-length", "16")],
        )
        assert sent[0]["status"] == 200 and sent[1]["body"] == b"16"

    def test_the_real_port_refuses_a_large_post(self, exchange_client):
        client, fake = exchange_client
        bounded = TestClient(connectors_composition.BodyLimit(client.app))
        response = bounded.post(
            EXCHANGE_PATH,
            content=b'{"lease_token": "' + b"x" * 8192 + b'", "operation": "read"}',
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 413 and fake.calls == []

    def test_the_server_is_bounded(self):
        config = connectors_composition.exchange_server_config(object())
        assert isinstance(config.app, connectors_composition.BodyLimit)
        assert config.app.limit == connectors_composition.MAX_BODY_BYTES == 4096
        assert (
            config.limit_concurrency
            == connectors_composition.MAX_CONCURRENT_CONNECTIONS
        )
        assert config.timeout_graceful_shutdown == 5
        assert config.ws == "none" and config.lifespan == "off"
